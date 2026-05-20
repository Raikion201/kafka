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
import org.apache.kafka.connect.manifest.codegen.model.ConfigTransformationSpec;
import org.apache.kafka.connect.manifest.codegen.model.IncrementalSyncSpec;
import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;
import org.apache.kafka.connect.manifest.codegen.model.PaginatorSpec;
import org.apache.kafka.connect.manifest.codegen.model.PartitionRouterSpec;
import org.apache.kafka.connect.manifest.codegen.model.RecordSelectorSpec;
import org.apache.kafka.connect.manifest.codegen.model.RequesterSpec;
import org.apache.kafka.connect.manifest.codegen.model.StreamSpec;
import org.apache.kafka.connect.manifest.codegen.runtime.transform.TransformationPipelineFactory;
import org.apache.kafka.connect.manifest.codegen.runtime.transform.config.ConfigTransformerFactory;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.lang.model.element.Modifier;

/**
 * Generates {@code XxxSourceTask.java} from an Airbyte manifest stream definition.
 *
 * <p>The generated task:
 * <ul>
 *   <li>Extends {@code SourceTask}.</li>
 *   <li>Uses {@code java.net.http.HttpClient} to call the configured endpoint.</li>
 *   <li>Navigates the {@code field_path} in the JSON response to extract records.</li>
 *   <li>Injects {@code request_parameters} from config (supports {@code {{ config['key'] }}} templates).</li>
 *   <li>Adds auth headers for Bearer, ApiKey, BasicHttp, and OAuth authenticator types.</li>
 *   <li>Paginates with CursorPagination, PageIncrement, or OffsetIncrement loops when present.</li>
 *   <li>Caches BasicHttp credentials in {@code start()} and OAuth tokens with expiry tracking.</li>
 *   <li>Retries transient HTTP 429/5xx errors up to 3 times with exponential backoff.</li>
 *   <li>Polls all streams in one {@code poll()} invocation.</li>
 *   <li>Publishes records as JSON strings to a topic named after the stream.</li>
 *   <li>Tracks DatetimeBasedCursor state per incremental stream across polls.</li>
 *   <li>Supports single-level SubstreamPartitionRouter (parent → child streams).</li>
 * </ul>
 */
public class TaskGenerator {

    private static final Pattern CONFIG_TEMPLATE      = JinjaSnippets.CONFIG_TEMPLATE;
    private static final Pattern STREAM_PARTITION_RE  = JinjaSnippets.STREAM_PARTITION_RE;

    private static final ClassName SOURCE_TASK =
        ClassName.get("org.apache.kafka.connect.source", "SourceTask");
    private static final ClassName SOURCE_RECORD =
        ClassName.get("org.apache.kafka.connect.source", "SourceRecord");
    private static final ClassName SCHEMA =
        ClassName.get("org.apache.kafka.connect.data", "Schema");
    private static final ClassName CONNECT_EXCEPTION =
        ClassName.get("org.apache.kafka.connect.errors", "ConnectException");
    private static final ClassName OBJECT_MAPPER =
        ClassName.get("com.fasterxml.jackson.databind", "ObjectMapper");
    private static final ClassName HTTP_CLIENT =
        ClassName.get("java.net.http", "HttpClient");
    private static final ClassName SHARED_HTTP_CLIENT =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime", "SharedHttpClient");
    private static final ClassName HTTP_REQUEST =
        ClassName.get("java.net.http", "HttpRequest");
    private static final ClassName HTTP_RESPONSE =
        ClassName.get("java.net.http", "HttpResponse");
    private static final ClassName URI_CLASS =
        ClassName.get("java.net", "URI");
    private static final ClassName CUSTOM_REGISTRY =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.customs",
            "CustomComponentRegistry");
    private static final ClassName CUSTOM_RETRIEVER =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.customs",
            "CustomRetriever");
    private static final ClassName CUSTOM_REQUESTER =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.customs",
            "CustomRequester");
    private static final ClassName CUSTOM_TRANSFORMATION =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.customs",
            "CustomTransformation");
    private static final ClassName CUSTOM_RECORD_EXTRACTOR =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.customs",
            "CustomRecordExtractor");
    private static final ClassName JSON_NODE =
        ClassName.get("com.fasterxml.jackson.databind", "JsonNode");
    private static final ClassName JINJA_RENDERER =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.jinja", "JinjaRenderer");
    private static final ClassName RETRY_POLICY =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry", "RetryPolicy");
    private static final ClassName DEFAULT_RETRY_POLICY =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry", "DefaultRetryPolicy");
    private static final ClassName COMPOSITE_RETRY_POLICY =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry", "CompositeRetryPolicy");
    private static final ClassName HTTP_RESPONSE_FILTER =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry", "HttpResponseFilter");
    private static final ClassName RESPONSE_ACTION =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry", "ResponseAction");
    private static final ClassName ERROR_RESOLUTION =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry", "ErrorResolution");
    private static final ClassName IGNORED_RESPONSES =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry", "IgnoredResponses");
    private static final ClassName BACKOFF_STRATEGY =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry.backoff", "BackoffStrategy");
    private static final ClassName BACKOFF_STRATEGY_CHAIN =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry.backoff", "BackoffStrategyChain");
    private static final ClassName EXPONENTIAL_BACKOFF =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry.backoff", "ExponentialBackoffStrategy");
    private static final ClassName CONSTANT_BACKOFF =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry.backoff", "ConstantBackoffStrategy");
    private static final ClassName WAIT_TIME_FROM_HEADER =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry.backoff",
            "WaitTimeFromHeaderBackoffStrategy");
    private static final ClassName WAIT_UNTIL_TIME_FROM_HEADER =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.retry.backoff",
            "WaitUntilTimeFromHeaderBackoffStrategy");

    private static final ClassName TRANSFORMATION_PIPELINE =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.transform",
            "TransformationPipeline");
    private static final ClassName TRANSFORMATION_PIPELINE_FACTORY =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.transform",
            "TransformationPipelineFactory");
    private static final ClassName CONFIG_TRANSFORMER =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.transform.config",
            "ConfigTransformer");
    private static final ClassName CONFIG_TRANSFORMER_FACTORY =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime.transform.config",
            "ConfigTransformerFactory");

    private static final ClassName BODY_PUBLISHERS =
        ClassName.get("java.net.http", "HttpRequest.BodyPublishers");
    private static final ClassName STD_CHARSETS =
        ClassName.get("java.nio.charset", "StandardCharsets");
    private static final ClassName DATETIME_WINDOW_HELPER =
        ClassName.get("org.apache.kafka.connect.manifest.codegen.runtime", "DatetimeWindowHelper");

    /** Used at codegen time to serialize nested {@code request_body_json} values to JSON literals. */
    private static final com.fasterxml.jackson.databind.ObjectMapper CODEGEN_MAPPER =
        new com.fasterxml.jackson.databind.ObjectMapper();

    private static final String APP_VERSION = "1.0.0";

    /** Monotonically-increasing counter used to give unique variable names in emitNestedBodyPut. */
    private int bodyPutSeq = 0;

    /**
     * Set to the Java variable names for the window start and end when generating body code for
     * a windowed (step-based) DatetimeBasedCursor stream, so that body templates referencing
     * {@code stream_interval} or {@code stream_slice} can be rendered with the correct context.
     * Null when not inside a windowed stream's body generation.
     */
    private String currentWindowStartVar = null;
    private String currentWindowEndVar = null;

    /** Keys declared in the current manifest's spec.connection_specification.properties.
     *  Set at the start of {@link #generate} so credential resolution can fall back to
     *  {@code ""} for Airbyte sentinel keys like {@code nothing} that have no real property. */
    private java.util.Set<String> currentSpecPropKeys = java.util.Collections.emptySet();

    /** Required keys from spec.connection_specification.required — used to detect optional
     *  config keys in URL paths so the codegen can emit a skip-guard when they are missing. */
    private java.util.Set<String> currentSpecRequiredKeys = java.util.Collections.emptySet();

    /**
     * Generate the source task class source file.
     *
     * @param spec    the parsed manifest
     * @param pkgName target Java package
     * @return a {@link JavaFile} ready to be written to disk
     * @throws CodegenException if the spec is malformed or a stream has no retriever
     */
    public JavaFile generate(ManifestSpec spec, String pkgName) throws CodegenException {
        this.currentSpecPropKeys = (spec.getSpec() != null
            && spec.getSpec().getConnectionSpecification() != null)
            ? spec.getSpec().getConnectionSpecification().getProperties().keySet()
            : java.util.Collections.emptySet();
        this.currentSpecRequiredKeys = (spec.getSpec() != null
            && spec.getSpec().getConnectionSpecification() != null)
            ? new java.util.HashSet<>(spec.getSpec().getConnectionSpecification().getRequired())
            : java.util.Collections.emptySet();
        List<StreamSpec> streams = spec.resolvedStreams();
        if (streams.isEmpty()) {
            // No usable streams (all dropped during parser tolerance, or only unsupported
            // dynamic-stream patterns). Emit a stub task so the connector still loads in
            // Connect and fails fast on start() with a clear "unsupported" message.
            String baseName = ManifestSpec.toClassName(spec.connectorClassName().replace("Source", ""));
            String taskClassName = baseName + "SourceTask";
            String configClassName = baseName + "ConnectorConfig";
            ClassName configClass = ClassName.get(pkgName, configClassName);
            TypeSpec stub = GenericDynamicStreamStub.build(taskClassName, configClass,
                spec.connectorClassName());
            return JavaFile.builder(pkgName, stub).skipJavaLangImports(true).build();
        }
        for (StreamSpec stream : streams) {
            if (stream.getRetriever() == null) {
                throw new CodegenException("Stream '" + stream.getName() + "' has no retriever");
            }
            // Retrievers with a custom class_name are dispatched to CustomComponentRegistry
            // and do not require a manifest-defined requester.
            if (stream.getRetriever().getClassName() == null
                && stream.getRetriever().getRequester() == null) {
                throw new CodegenException("Stream '" + stream.getName() + "' retriever has no requester");
            }
        }

        // Dynamic streams (Airbyte runtime stream discovery) take a dedicated codegen path
        // because the per-stream URL/topic/range vary at runtime, not at codegen time.
        if (streams.get(0).isDynamic()) {
            return new DynamicStreamTaskGenerator().generate(spec, pkgName, streams.get(0));
        }

        String baseName = ManifestSpec.toClassName(
            spec.connectorClassName().replace("Source", ""));
        String taskClassName = baseName + "SourceTask";
        String configClassName = baseName + "ConnectorConfig";
        ClassName configClass = ClassName.get(pkgName, configClassName);

        // Auth is sourced from the first stream that exposes a manifest-defined requester.
        // Streams whose retriever or requester is dispatched to CustomComponentRegistry
        // bring their own auth (the registered Java impl owns it), so they are skipped here.
        StreamSpec authSource = streams.stream()
            .filter(s -> !isCustomDispatch(s) && s.getRetriever().getRequester() != null)
            .findFirst()
            .orElse(null);
        AuthenticatorSpec auth = authSource == null
            ? null : authSource.getRetriever().getRequester().getAuthenticator();
        if (auth != null && auth.isSelective()) {
            auth = auth.resolveEffectiveLeaf();
        }

        ParameterizedTypeName mapStringString = ParameterizedTypeName.get(
            ClassName.get("java.util", "Map"),
            ClassName.get(String.class),
            ClassName.get(String.class)
        );
        ParameterizedTypeName listOfSourceRecord = ParameterizedTypeName.get(
            ClassName.get("java.util", "List"),
            SOURCE_RECORD
        );

        TypeSpec.Builder typeBuilder = TypeSpec.classBuilder(taskClassName)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(SOURCE_TASK);

        // Filter streams:
        // - Exactly 1 SubstreamPartitionRouter → supported child stream, include it.
        // - ≥1 ListPartitionRouter (no substream) → fan-out over static values, include it.
        // - 0 routers AND path has no stream_partition template → normal stream, include.
        // - 0 routers AND path contains stream_partition → broken ref (no router), skip.
        // - >1 SubstreamPartitionRouter → include only when nested pattern matches
        //   (router N's parent is itself a substream of router N-1's parent).
        List<StreamSpec> runnableStreams = streams.stream()
            .filter(s -> {
                if (isCustomDispatch(s)) return true;
                long substreamCount = s.getRetriever().getPartitionRouter().stream()
                    .filter(PartitionRouterSpec::isSubstream).count();
                long listCount = s.getRetriever().getListRouters().size();
                if (substreamCount == 1 && listCount == 0) return true;
                if (substreamCount > 1) return multiSubstreamChain(s, spec) != null;
                if (substreamCount == 0 && listCount > 0) return true;
                String p = s.getRetriever().getRequester().getPath();
                return p == null || (!p.contains("stream_partition") && !p.contains("stream_slice"));
            })
            .collect(java.util.stream.Collectors.toList());

        boolean hasListCycle = runnableStreams.stream().anyMatch(
            s -> !isCustomDispatch(s)
                && s.getRetriever().getRequester().listCycleConfigField() != null);
        addClassFields(typeBuilder, configClass, auth, hasListCycle, runnableStreams);

        typeBuilder.addMethod(
            MethodSpec.methodBuilder("version")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addStatement("return $S", APP_VERSION)
                .build()
        );

        typeBuilder.addMethod(buildStart(mapStringString, configClass, auth, runnableStreams, spec));

        typeBuilder.addMethod(buildPollAll(runnableStreams, listOfSourceRecord));

        for (StreamSpec stream : runnableStreams) {
            typeBuilder.addMethod(buildStreamPollMethod(stream, configClass, auth, listOfSourceRecord));
            if (isCustomDispatch(stream)) continue;
            List<PartitionRouterSpec> chain = multiSubstreamChain(stream, spec);
            if (chain != null) {
                typeBuilder.addMethod(buildNestedFetchPartitionKeys(stream, chain, auth, spec));
                continue;
            }
            PartitionRouterSpec router = stream.getRetriever().getSubstreamRouter();
            if (router != null) {
                typeBuilder.addMethod(buildFetchPartitionKeys(stream, router, auth, spec));
            }
        }

        boolean hasHttpStream = runnableStreams.stream().anyMatch(s -> !isCustomDispatch(s));
        if (hasHttpStream) {
            String baseUrl = runnableStreams.stream()
                .filter(s -> !isCustomDispatch(s))
                .findFirst().get()
                .getRetriever().getRequester().effectiveBaseUrl();
            addAuthHelperMethods(typeBuilder, configClass, auth, baseUrl);
            typeBuilder.addMethod(buildSendWithRetry());
        }
        typeBuilder.addMethod(buildStop());
        typeBuilder.addMethod(buildJinjaCtx());
        typeBuilder.addMethod(buildJinjaCtxWithRecord());
        typeBuilder.addMethod(buildJinjaCtxForStream(runnableStreams));
        typeBuilder.addMethod(buildJinjaCtxWithSlice());

        return JavaFile.builder(pkgName, typeBuilder.build())
            .skipJavaLangImports(true)
            .addStaticImport(JINJA_RENDERER, "render")
            .addStaticImport(JINJA_RENDERER, "normalizeUrl")
            .build();
    }

    private void addClassFields(
        TypeSpec.Builder typeBuilder,
        ClassName configClass,
        AuthenticatorSpec auth,
        boolean hasListCycle,
        List<StreamSpec> streams
    ) {
        typeBuilder.addField(
            FieldSpec.builder(OBJECT_MAPPER, "MAPPER", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer("new $T()", OBJECT_MAPPER)
                .build()
        );
        typeBuilder.addField(configClass, "config", Modifier.PRIVATE);
        typeBuilder.addField(
            FieldSpec.builder(HTTP_CLIENT, "httpClient", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer("$T.INSTANCE", SHARED_HTTP_CLIENT)
                .build());

        // Per-stream RetryPolicy/BackoffStrategy are constructed at each sendWithRetry call
        // site so that each declarative stream's own error_handler (response_filters +
        // backoff_strategies) is applied — matching Airbyte's per-HttpRequester model.

        if (auth != null && auth.isBasicHttp()) {
            typeBuilder.addField(String.class, "cachedCredentials", Modifier.PRIVATE);
        }
        if (auth != null && auth.isOAuth()) {
            typeBuilder.addField(
                FieldSpec.builder(String.class, "cachedToken", Modifier.PRIVATE, Modifier.VOLATILE)
                    .build()
            );
            typeBuilder.addField(
                FieldSpec.builder(long.class, "tokenExpiryMs", Modifier.PRIVATE, Modifier.VOLATILE)
                    .initializer("0L")
                    .build()
            );
        }
        if (auth != null && auth.isSessionToken()) {
            typeBuilder.addField(
                FieldSpec.builder(String.class, "cachedSessionToken", Modifier.PRIVATE, Modifier.VOLATILE)
                    .build()
            );
        }
        if (auth != null && auth.isLegacySessionToken()) {
            typeBuilder.addField(
                FieldSpec.builder(String.class, "cachedLegacyToken", Modifier.PRIVATE, Modifier.VOLATILE)
                    .build()
            );
        }
        if (hasListCycle) {
            typeBuilder.addField(
                FieldSpec.builder(int.class, "tickerIndex", Modifier.PRIVATE, Modifier.VOLATILE)
                    .initializer("-1")
                    .build()
            );
        }

        // Config-time transformer (applied once in start() before any config reads).
        typeBuilder.addField(CONFIG_TRANSFORMER, "configTransformer", Modifier.PRIVATE);
        // Stores the (possibly config-transformed) config values, used as the Jinja config context.
        ParameterizedTypeName mapStrObj = ParameterizedTypeName.get(
            ClassName.get("java.util", "Map"),
            ClassName.get(String.class), ClassName.get(Object.class));
        typeBuilder.addField(mapStrObj, "configValues", Modifier.PRIVATE);

        // Per-stream transformation pipelines (applied to each record before emission).
        for (StreamSpec s : streams) {
            typeBuilder.addField(TRANSFORMATION_PIPELINE,
                pipelineFieldName(s.getName()), Modifier.PRIVATE);
        }

        // DatetimeBasedCursor: one volatile String cursor field per incremental stream.
        for (StreamSpec s : streams) {
            IncrementalSyncSpec inc = s.getIncrementalSync();
            if (inc != null && inc.isDatetimeBased()) {
                typeBuilder.addField(
                    FieldSpec.builder(String.class, cursorFieldName(s.getName()),
                        Modifier.PRIVATE, Modifier.VOLATILE)
                        .build()
                );
            }
        }

        // SubstreamPartitionRouter: partition key list + current index per child stream.
        // Single-router child streams hold List<String>; multi-router (nested) child streams
        // hold List<Map<String,String>> so each entry carries one value per partition_field.
        ParameterizedTypeName listString = ParameterizedTypeName.get(
            ClassName.get("java.util", "List"), ClassName.get(String.class));
        ParameterizedTypeName mapStringStringField = ParameterizedTypeName.get(
            ClassName.get("java.util", "Map"),
            ClassName.get(String.class), ClassName.get(String.class));
        ParameterizedTypeName listMapStringString = ParameterizedTypeName.get(
            ClassName.get("java.util", "List"), mapStringStringField);
        for (StreamSpec s : streams) {
            if (isCustomDispatch(s)) continue;
            long subCount = s.getRetriever().getPartitionRouter().stream()
                .filter(PartitionRouterSpec::isSubstream).count();
            boolean isNested = subCount > 1;
            PartitionRouterSpec router = s.getRetriever().getSubstreamRouter();
            if (isNested || router != null) {
                typeBuilder.addField(
                    FieldSpec.builder(isNested ? listMapStringString : listString,
                            partitionKeysFieldName(s.getName()),
                        Modifier.PRIVATE, Modifier.VOLATILE)
                        .build()
                );
                typeBuilder.addField(
                    FieldSpec.builder(int.class, partitionIdxFieldName(s.getName()),
                        Modifier.PRIVATE, Modifier.VOLATILE)
                        .initializer("-1")
                        .build()
                );
            }
        }
    }

    private MethodSpec buildStart(
        ParameterizedTypeName mapStringString,
        ClassName configClass,
        AuthenticatorSpec auth,
        List<StreamSpec> streams,
        ManifestSpec spec
    ) {
        MethodSpec.Builder m = MethodSpec.methodBuilder("start")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(mapStringString, "props")
            .addStatement("this.config = new $T(props)", configClass);

        // Config-time transforms — applied once before any other initialization reads config.
        m.addStatement("this.configTransformer = $T.fromJson($S)",
            CONFIG_TRANSFORMER_FACTORY, configTransformsJson(spec));
        // Build the Jinja config map from two sources:
        //  (1) originalsStrings() — the raw user-supplied props, including fields not declared
        //      in the spec's ConfigDef (e.g. appfollow's `api_key`, which the spec declares as
        //      `api_secret`). Mirrors Airbyte Python's JinjaInterpolation.eval(), which passes
        //      the raw config dict unfiltered (airbyte_cdk/sources/declarative/interpolation/jinja.py:90).
        //  (2) ConfigDef defaults via values() — for declared fields the user omitted (e.g.
        //      shortcut's `query` default `title:Our first Epic`). Airbyte's Python config
        //      dict already has spec defaults merged in by the spec layer; we have to merge
        //      them ourselves because Kafka Connect splits raw input from typed/defaulted values.
        m.addStatement("$T<$T, $T> _cfgMap = new $T<$T, $T>(this.config.originalsStrings())",
            Map.class, String.class, Object.class,
            java.util.LinkedHashMap.class, String.class, Object.class);
        m.addStatement(
            "this.config.values().forEach((_k, _v) -> { if (_v != null) _cfgMap.putIfAbsent(_k, _v); })");
        m.beginControlFlow("if (!this.configTransformer.isNoop())")
            .addStatement("this.configTransformer.apply(_cfgMap)")
            .endControlFlow();
        m.addStatement("this.configValues = _cfgMap");

        // Per-stream transformation pipelines.
        for (StreamSpec s : streams) {
            String transformsJson = TransformationPipelineFactory.toJson(s.getTransformations());
            String filterCondition = filterConditionFor(s);
            if (filterCondition != null) {
                m.addStatement("this.$L = $T.fromJson($S, $S, this.config.originalsStrings())",
                    pipelineFieldName(s.getName()), TRANSFORMATION_PIPELINE_FACTORY,
                    transformsJson, filterCondition);
            } else {
                m.addStatement("this.$L = $T.fromJson($S, this.config.originalsStrings())",
                    pipelineFieldName(s.getName()), TRANSFORMATION_PIPELINE_FACTORY,
                    transformsJson);
            }
        }

        if (auth != null && auth.isBasicHttp()) {
            String userExpr = interpolateTemplate(auth.getUsername());
            String passExpr = interpolateTemplate(auth.getPassword());
            m.addStatement(
                "this.cachedCredentials = $T.getEncoder().encodeToString(\n"
                    + "        ($L + \":\" + $L).getBytes($T.UTF_8))",
                ClassName.get("java.util", "Base64"),
                userExpr, passExpr,
                ClassName.get("java.nio.charset", "StandardCharsets")
            );
        }
        if (auth != null && auth.isSessionToken()) {
            m.beginControlFlow("try");
            m.addStatement("loginAndCacheSessionToken()");
            m.nextControlFlow("catch ($T e)", Exception.class);
            m.addStatement("throw new $T(\"Failed to obtain session token\", e)", CONNECT_EXCEPTION);
            m.endControlFlow();
        }
        if (auth != null && auth.isLegacySessionToken()) {
            m.beginControlFlow("try");
            m.addStatement("loginAndCacheLegacyToken()");
            m.nextControlFlow("catch ($T e)", Exception.class);
            m.addStatement("throw new $T(\"Failed to obtain legacy session token\", e)", CONNECT_EXCEPTION);
            m.endControlFlow();
        }

        // Pre-fetch parent partition keys for each substream (single-router or nested).
        for (StreamSpec s : streams) {
            if (isCustomDispatch(s)) continue;
            long subCount = s.getRetriever().getPartitionRouter().stream()
                .filter(PartitionRouterSpec::isSubstream).count();
            PartitionRouterSpec router = s.getRetriever().getSubstreamRouter();
            if (router != null || subCount > 1) {
                String keysField = partitionKeysFieldName(s.getName());
                String fetchMethod = "fetch" + ManifestSpec.toClassName(s.getName()) + "PartitionKeys";
                m.beginControlFlow("try");
                m.addStatement("this.$L = $L()", keysField, fetchMethod);
                m.nextControlFlow("catch ($T _e)", Exception.class);
                m.addStatement("this.$L = new $T<>()", keysField, ArrayList.class);
                m.endControlFlow();
            }
        }

        return m.build();
    }

    private MethodSpec buildPollAll(List<StreamSpec> streams, ParameterizedTypeName listOfSourceRecord) {
        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("$T<$T> all = new $T<>()", List.class, SOURCE_RECORD, ArrayList.class);
        for (StreamSpec stream : streams) {
            String pollMethod = "poll" + ManifestSpec.toClassName(stream.getName());
            // Wrap per-stream: a 4xx/account-limitation on one stream skips it rather than
            // failing the whole task (mirrors Airbyte CDK's per-stream error isolation).
            body.beginControlFlow("try");
            body.addStatement("all.addAll($L())", pollMethod);
            body.nextControlFlow("catch ($T _se)", CONNECT_EXCEPTION);
            body.addStatement("$T.err.println(\"[WARN] stream $L skipped: \" + _se.getMessage()"
                + " + (_se.getCause() != null ? \" cause=\" + _se.getCause() : \"\"))",
                System.class, stream.getName());
            body.endControlFlow();
        }
        body.addStatement("return all");
        return MethodSpec.methodBuilder("poll")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(listOfSourceRecord)
            .addException(InterruptedException.class)
            .addCode(body.build())
            .build();
    }

    /**
     * True when the stream's retriever or requester has a {@code class_name} that must be
     * dispatched to {@link org.apache.kafka.connect.manifest.codegen.runtime.customs.CustomComponentRegistry}
     * at runtime instead of being emitted as direct HTTP code.
     */
    private static boolean isCustomDispatch(StreamSpec s) {
        if (s.getRetriever() == null) return false;
        if (s.getRetriever().getClassName() != null) return true;
        RequesterSpec r = s.getRetriever().getRequester();
        return r != null && r.getClassName() != null;
    }

    private MethodSpec buildStreamPollMethod(
        StreamSpec stream,
        ClassName configClass,
        AuthenticatorSpec auth,
        ParameterizedTypeName listOfSourceRecord
    ) throws CodegenException {
        if (isCustomDispatch(stream)) {
            return buildCustomComponentPollMethod(stream, listOfSourceRecord);
        }
        RequesterSpec requester = stream.getRetriever().getRequester();

        // List-cycle pattern: path/params use config['field'].split(',')[page] as array index.
        String listCycleField = requester.listCycleConfigField();
        if (listCycleField != null) {
            return buildListCyclePollMethod(stream, configClass, auth, listOfSourceRecord, listCycleField);
        }

        // Nested SubstreamPartitionRouter: ≥2 substream routers in chain. Uses Map<String,String>
        // _partition values per stream slice.
        long subCount = stream.getRetriever().getPartitionRouter().stream()
            .filter(PartitionRouterSpec::isSubstream).count();
        if (subCount > 1) {
            return buildNestedSubstreamPollMethod(stream, configClass, auth, listOfSourceRecord);
        }
        // SubstreamPartitionRouter: child stream whose path contains a partition variable.
        PartitionRouterSpec router = stream.getRetriever().getSubstreamRouter();
        if (router != null) {
            return buildSubstreamPollMethod(stream, configClass, auth, listOfSourceRecord, router);
        }

        // ListPartitionRouter: fan out over a static list of values.
        List<PartitionRouterSpec> listRouters = stream.getRetriever().getListRouters();
        if (!listRouters.isEmpty()) {
            return buildListRouterPollMethod(stream, configClass, auth, listOfSourceRecord, listRouters);
        }

        // Normal stream (with optional DatetimeBasedCursor).
        String baseUrl = requester.effectiveBaseUrl();
        String path = requester.getPath();
        // Don't add "/" when path is a pure Jinja expression (starts with {{ or {%):
        // the expression may evaluate to a string starting with "/" at runtime,
        // which would produce a double-slash if we also append "/" here.
        boolean pathIsJinjaExpr = path.startsWith("{{") || path.startsWith("{%");
        if (!baseUrl.isEmpty() && !baseUrl.endsWith("/") && !path.isEmpty()
                && !path.startsWith("/") && !pathIsJinjaExpr) {
            baseUrl = baseUrl + "/";
        }
        String streamName = stream.getName();
        PaginatorSpec paginator = stream.getRetriever().getPaginator();
        Map<String, String> requestParams = requester.getRequestParameters();
        List<String> fieldPath = extractFieldPath(stream);
        IncrementalSyncSpec incrementalSync = stream.getIncrementalSync();
        boolean isIncremental = incrementalSync != null && incrementalSync.isDatetimeBased();
        String cursorVar = isIncremental ? cursorFieldName(streamName) : null;

        String methodName = "poll" + ManifestSpec.toClassName(streamName);
        boolean hasPagination = paginator != null && !paginator.hasNoPagination();

        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("final $T streamName = $S", String.class, streamName);

        if (hasPagination) {
            body.add(buildPaginationInit(paginator, isIncremental));
        } else {
            body.addStatement("$T<$T> result = new $T<>()", List.class, SOURCE_RECORD, ArrayList.class);
        }

        // Restore or initialise DatetimeBasedCursor from offset store.
        if (isIncremental) {
            body.add(buildIncrementalInit(streamName, incrementalSync));
            // Window slicing: compute window end once here so it's in scope for both
            // URL building (end_time_option injection) and cursor advancement after fetch.
            if (incrementalSync.hasStep()) {
                emitWindowEndComputation(body, incrementalSync, cursorVar);
            }
        }

        // Guard: skip streams whose URL path depends on optional config keys that aren't set.
        emitOptionalPathConfigGuards(body, path, ClassName.get("java.util", "List"), SOURCE_RECORD);
        body.add(buildUrlBlock(baseUrl, path, requestParams, paginator, hasPagination, auth,
            incrementalSync, cursorVar, stream));
        body.beginControlFlow("try");
        boolean bodyUsesStreamInterval = requester.getRequestBodyJson() != null
            && requester.getRequestBodyJson().values().stream()
                .anyMatch(v -> v instanceof String s
                    && (s.contains("stream_interval") || s.contains("stream_slice")));
        if (incrementalSync != null && incrementalSync.hasStep()) {
            currentWindowStartVar = cursorVar;
            currentWindowEndVar = "_windowEnd";
        } else if (isIncremental && bodyUsesStreamInterval) {
            // No step: single wide window from cursor to now; stream_interval still needed in body
            String wFmt = incrementalSync.getDatetimeFormat() != null
                ? incrementalSync.getDatetimeFormat() : "%s";
            body.addStatement("$T _windowEnd = $T.formatDate($T.now($T.UTC), $S)",
                String.class, DATETIME_WINDOW_HELPER,
                ClassName.get("java.time", "ZonedDateTime"),
                ClassName.get("java.time", "ZoneOffset"), wFmt);
            currentWindowStartVar = cursorVar;
            currentWindowEndVar = "_windowEnd";
        }
        buildRequestStatement(body, auth, paginator, requester);
        currentWindowStartVar = null;
        currentWindowEndVar = null;
        body.add(buildFetchBlock(paginator, stream));
        if (hasPagination && paginator.isCursor()) {
            body.add(buildCursorStateUpdate(paginator));
        }
        if (isCustomExtractor(stream)) {
            body.add(buildCustomExtractorNavBlock(stream.getRetriever().getRecordSelector().getExtractor().getClassName()));
        } else {
            body.add(buildFieldPathNav(fieldPath, hasPagination));
        }
        body.add(buildNormalizeAndCollect(hasPagination, paginator, cursorVar,
            isIncremental ? expandStreamParams(incrementalSync.getCursorField(), stream) : null,
            pipelineFieldName(streamName), incrementalSync));
        body.nextControlFlow("catch ($T e)", InterruptedException.class);
        body.addStatement("$T.currentThread().interrupt()", Thread.class);
        body.addStatement(
            "throw new $T(\"Interrupted while polling \" + streamName, e)", CONNECT_EXCEPTION);
        body.nextControlFlow("catch ($T e)", Exception.class);
        body.addStatement("throw new $T(\"Failed to poll \" + streamName, e)", CONNECT_EXCEPTION);
        body.endControlFlow();

        if (hasPagination) {
            body.add(buildPaginationLoopClose(paginator));
        } else {
            body.addStatement("return result");
        }

        return MethodSpec.methodBuilder(methodName)
            .addModifiers(Modifier.PRIVATE)
            .returns(listOfSourceRecord)
            .addException(InterruptedException.class)
            .addCode(body.build())
            .build();
    }

    /**
     * Generates the poll method for a child stream that iterates over parent partition keys.
     * One partition key is consumed per {@code poll()} call; the index wraps around after
     * all keys have been processed, and keys are re-fetched at that point.
     */
    private MethodSpec buildSubstreamPollMethod(
        StreamSpec stream,
        ClassName configClass,
        AuthenticatorSpec auth,
        ParameterizedTypeName listOfSourceRecord,
        PartitionRouterSpec router
    ) throws CodegenException {
        RequesterSpec requester = stream.getRetriever().getRequester();
        String baseUrl = requester.effectiveBaseUrl();
        String rawPath = requester.getPath();
        String streamName = stream.getName();
        String methodName = "poll" + ManifestSpec.toClassName(streamName);
        PaginatorSpec paginator = stream.getRetriever().getPaginator();
        List<String> fieldPath = extractFieldPath(stream);
        boolean hasPagination = paginator != null && !paginator.hasNoPagination();

        String keysField = partitionKeysFieldName(streamName);
        String idxField  = partitionIdxFieldName(streamName);

        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("final $T streamName = $S", String.class, streamName);
        body.addStatement("$T<$T> result = new $T<>()", List.class, SOURCE_RECORD, ArrayList.class);

        // On first poll, restore partition index from offset store.
        body.beginControlFlow("if ($L < 0)", idxField);
        body.addStatement(
            "$T<$T, $T> _stored = context.offsetStorageReader().offset($T.of($S, streamName))",
            Map.class, String.class, Object.class, Map.class, "stream");
        body.addStatement("$L = 0", idxField);
        body.beginControlFlow(
            "if (_stored != null && _stored.get($S) instanceof $T _p)", "partition_idx", Number.class);
        body.addStatement("$L = _p.intValue()", idxField);
        body.endControlFlow();
        body.endControlFlow();

        // Guard: no partition keys fetched yet.
        body.beginControlFlow("if ($L == null || $L.isEmpty())", keysField, keysField);
        body.addStatement("return result");
        body.endControlFlow();

        // Wrap around when we've processed all keys; re-fetch on next cycle.
        body.beginControlFlow("if ($L >= $L.size())", idxField, keysField);
        body.addStatement("$L = 0", idxField);
        body.beginControlFlow("try");
        body.addStatement("$L = fetch$LPartitionKeys()", keysField, ManifestSpec.toClassName(streamName));
        body.nextControlFlow("catch ($T _e)", Exception.class);
        body.endControlFlow();
        body.beginControlFlow("if ($L == null || $L.isEmpty())", keysField, keysField);
        body.addStatement("return result");
        body.endControlFlow();
        body.endControlFlow();

        body.addStatement("$T _partitionKey = $L.get($L)", String.class, keysField, idxField);
        body.addStatement("int _nextIdx = ($L + 1 >= $L.size()) ? 0 : $L + 1", idxField, keysField, idxField);

        // Declare pagination state variables (fresh per partition-key call, not restored from store).
        if (hasPagination && paginator != null) {
            if (paginator.isCursor()) {
                body.addStatement("$T nextCursor = null", String.class);
                // RequestPath paginators use the cursor as the full next URL — declare it too.
                if (isRequestPath(paginator)) {
                    // `url` is declared by buildSubstreamUrlBlock for RequestPath paginators.
                }
            } else if (paginator.isPageIncrement()) {
                int start = paginator.getPaginationStrategy() != null
                    ? paginator.getPaginationStrategy().getStartFromPage() : 1;
                body.addStatement("final int startPage = $L", start);
                body.addStatement("int page = startPage");
                body.addStatement("final int pageLimit = $L", paginator.pageSize());
            } else if (paginator.isOffsetIncrement()) {
                body.addStatement("int offset = 0");
                body.addStatement("final int pageLimit = $L", paginator.pageSize());
            }
        }

        // Build URL substituting {{ stream_partition.X }} → _partitionKey.
        body.add(buildSubstreamUrlBlock(baseUrl, rawPath, paginator, hasPagination, router));

        body.beginControlFlow("try");
        buildRequestStatement(body, auth, paginator, requester);
        body.add(buildFetchBlock(paginator, stream));
        if (hasPagination && paginator.isCursor()) {
            body.add(buildCursorStateUpdate(paginator));
        }
        if (isCustomExtractor(stream)) {
            body.add(buildCustomExtractorNavBlock(stream.getRetriever().getRecordSelector().getExtractor().getClassName()));
        } else {
            body.add(buildFieldPathNav(fieldPath, hasPagination));
        }
        body.add(buildNormalizeAndCollect(hasPagination, paginator, null, null,
            pipelineFieldName(streamName)));

        // Advance partition index only after last pagination page is consumed.
        if (hasPagination && paginator.isCursor()) {
            body.beginControlFlow("if (nextCursor == null || nextCursor.isEmpty())");
            body.addStatement("$L = _nextIdx", idxField);
            body.endControlFlow();
        } else {
            body.addStatement("$L = _nextIdx", idxField);
        }

        body.nextControlFlow("catch ($T e)", InterruptedException.class);
        body.addStatement("$T.currentThread().interrupt()", Thread.class);
        body.addStatement("throw new $T(\"Interrupted while polling \" + streamName, e)", CONNECT_EXCEPTION);
        body.nextControlFlow("catch ($T e)", Exception.class);
        body.addStatement("throw new $T(\"Failed to poll \" + streamName, e)", CONNECT_EXCEPTION);
        body.endControlFlow();

        if (hasPagination) {
            body.add(buildPaginationLoopClose(paginator));
        } else {
            body.addStatement("return result");
        }

        return MethodSpec.methodBuilder(methodName)
            .addModifiers(Modifier.PRIVATE)
            .returns(listOfSourceRecord)
            .addException(InterruptedException.class)
            .addCode(body.build())
            .build();
    }

    /**
     * Builds URL initialisation for a child stream path that contains
     * {@code {{ stream_partition.X }}}. Splits the path on the Jinja2 template
     * and inserts the runtime {@code _partitionKey} value.
     * For RequestPath cursor paginators, assigns to the {@code url} variable.
     */
    private CodeBlock buildSubstreamUrlBlock(
        String baseUrl, String rawPath,
        PaginatorSpec paginator, boolean hasPagination,
        PartitionRouterSpec router
    ) {
        CodeBlock.Builder b = CodeBlock.builder();

        // Compute the base URL with partition variable substituted.
        Matcher m = STREAM_PARTITION_RE.matcher(rawPath);
        boolean noPartitionVar = !m.find();

        if (isRequestPath(paginator)) {
            // For RequestPath cursor, the cursor itself is the full URL for page 2+.
            // For the first page, build the URL from base + path.
            if (noPartitionVar) {
                String full = joinUrl(baseUrl, rawPath);
                if (containsJinja(full)) {
                    b.addStatement("$T url = (nextCursor != null) ? nextCursor : $L",
                        String.class, interpolateTemplate(full));
                } else {
                    b.addStatement("$T url = (nextCursor != null) ? nextCursor : $S",
                        String.class, full);
                }
            } else {
                String before = rawPath.substring(0, m.start());
                String after  = rawPath.substring(m.end());
                emitSubstreamBaseUrlDecl(b, "_baseUrl", joinUrl(baseUrl, before), after);
                b.addStatement("$T url = (nextCursor != null) ? nextCursor : _baseUrl", String.class);
            }
            return b.build();
        }

        // Standard urlBuilder path.
        if (noPartitionVar) {
            String full = joinUrl(baseUrl, rawPath);
            if (containsJinja(full)) {
                b.addStatement("$T urlBuilder = new $T($L)",
                    StringBuilder.class, StringBuilder.class, interpolateTemplate(full));
            } else {
                b.addStatement("$T urlBuilder = new $T($S)",
                    StringBuilder.class, StringBuilder.class, full);
            }
        } else {
            String before = rawPath.substring(0, m.start());
            String after  = rawPath.substring(m.end());
            emitSubstreamUrlBuilderDecl(b, joinUrl(baseUrl, before), after);
        }

        // Inject parent_stream_configs[].request_option (Python substream_partition_router.py
        // lines 162-176). When inject_into=request_parameter, append the parent partition value
        // as ?<field_name>=<URL-encoded _partitionKey>.
        boolean firstParam = !joinUrl(baseUrl, rawPath).contains("?");
        PartitionRouterSpec.RequestOptionSpec parentOpt = router != null ? router.parentRequestOption() : null;
        if (parentOpt != null && parentOpt.isRequestParameter() && parentOpt.getFieldName() != null) {
            b.addStatement("urlBuilder.append($S).append($T.encode(_partitionKey, $T.UTF_8))",
                (firstParam ? "?" : "&") + parentOpt.getFieldName() + "=",
                ClassName.get("java.net", "URLEncoder"),
                ClassName.get("java.nio.charset", "StandardCharsets"));
            firstParam = false;
        }

        if (hasPagination && paginator != null) {
            appendPaginationParams(b, paginator, firstParam,
                isBodyInjectedJson(paginator) || isBodyInjectedData(paginator));
        }
        return b.build();
    }

    /**
     * Generates the private {@code fetchXxxPartitionKeys()} method that fetches all records
     * from the parent stream and returns a list of the extracted parent key values.
     */
    private MethodSpec buildFetchPartitionKeys(
        StreamSpec childStream,
        PartitionRouterSpec router,
        AuthenticatorSpec auth,
        ManifestSpec spec
    ) throws CodegenException {
        String parentStreamName = router.parentStreamName();
        String parentKey        = router.parentKey();
        String methodName = "fetch" + ManifestSpec.toClassName(childStream.getName()) + "PartitionKeys";

        ParameterizedTypeName listString = ParameterizedTypeName.get(
            ClassName.get("java.util", "List"), ClassName.get(String.class));

        StreamSpec parentStream = spec.resolvedStreams().stream()
            .filter(s -> parentStreamName != null && parentStreamName.equals(s.getName()))
            .findFirst().orElse(null);

        if (parentStream == null || parentStream.getRetriever() == null) {
            return MethodSpec.methodBuilder(methodName)
                .addModifiers(Modifier.PRIVATE)
                .addException(Exception.class)
                .returns(listString)
                .addStatement("return $T.emptyList()", Collections.class)
                .build();
        }

        RequesterSpec parentRequester = parentStream.getRetriever().getRequester();
        if (parentRequester == null) {
            return MethodSpec.methodBuilder(methodName)
                .addModifiers(Modifier.PRIVATE)
                .addException(Exception.class)
                .returns(listString)
                .addStatement("return $T.emptyList()", Collections.class)
                .build();
        }
        String baseUrl = parentRequester.effectiveBaseUrl();
        String path    = parentRequester.getPath();
        PaginatorSpec parentPaginator = parentStream.getRetriever().getPaginator();
        List<String> fieldPath = extractFieldPath(parentStream);

        ParameterizedTypeName arrayListString = ParameterizedTypeName.get(
            ClassName.get("java.util", "ArrayList"), ClassName.get(String.class));

        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("$T keys = new $T()", listString, arrayListString);
        body.addStatement("$T nextCursor = null", String.class);

        body.beginControlFlow("do");
        String fullParentUrl = joinUrl(baseUrl, path);
        if (containsJinja(fullParentUrl)) {
            body.addStatement("$T urlBuilder = new $T($L)",
                StringBuilder.class, StringBuilder.class, interpolateTemplate(fullParentUrl));
        } else {
            body.addStatement("$T urlBuilder = new $T($S)",
                StringBuilder.class, StringBuilder.class, fullParentUrl);
        }

        // Append cursor pagination token if present (not RequestPath).
        if (parentPaginator != null && parentPaginator.isCursor()
                && parentPaginator.getPageTokenOption() != null
                && !parentPaginator.getPageTokenOption().isRequestPath()) {
            body.beginControlFlow("if (nextCursor != null && !nextCursor.isEmpty())");
            body.addStatement("urlBuilder.append($S + nextCursor)", "?" + parentPaginator.pageParamName() + "=");
            body.endControlFlow();
        }

        // When the parent stream has a windowed DatetimeBasedCursor, its request_body_json may
        // reference stream_interval.  For the key-fetcher we use a single wide window: from the
        // configured start date to "now", so all parent records are included in one pass.
        IncrementalSyncSpec parentSync = parentStream.getIncrementalSync();
        boolean parentIsWindowed = parentSync != null && parentSync.isDatetimeBased()
            && parentRequester.getRequestBodyJson() != null
            && parentRequester.getRequestBodyJson().values().stream()
                .anyMatch(v -> v instanceof String s
                    && (s.contains("stream_interval") || s.contains("stream_slice")));
        if (parentIsWindowed) {
            String fmt = parentSync.getDatetimeFormat() != null ? parentSync.getDatetimeFormat() : "%s";
            IncrementalSyncSpec.DatetimeSpec startDt = parentSync.getStartDatetime();
            String startFmt = (startDt != null && startDt.getDatetimeFormat() != null)
                ? startDt.getDatetimeFormat() : fmt;
            String startExpr = (startDt != null && startDt.getDatetime() != null)
                ? interpolateTemplate(startDt.getDatetime()) : "\"0\"";
            if (!startFmt.equals(fmt)) {
                body.addStatement("$T _kfWindowStart = $T.formatDate($T.parseDate($L, $S), $S)",
                    String.class, DATETIME_WINDOW_HELPER, DATETIME_WINDOW_HELPER, startExpr, startFmt, fmt);
            } else {
                body.addStatement("$T _kfWindowStart = $L", String.class, startExpr);
            }
            body.addStatement("$T _kfWindowEnd = $T.formatDate($T.now($T.UTC), $S)",
                String.class, DATETIME_WINDOW_HELPER,
                ClassName.get("java.time", "ZonedDateTime"),
                ClassName.get("java.time", "ZoneOffset"), fmt);
            currentWindowStartVar = "_kfWindowStart";
            currentWindowEndVar = "_kfWindowEnd";
            // jinjaCtxWithSlice expects a local 'streamName' variable in scope
            body.addStatement("$T streamName = $S", String.class, parentStreamName);
        }

        // Build and send request (same auth as child stream).
        buildRequestStatement(body, auth, null, parentRequester);
        currentWindowStartVar = null;
        currentWindowEndVar = null;
        emitSendWithRetryCall(body, "response", "request", parentRequester.getErrorHandler());
        body.beginControlFlow("if (response.statusCode() < 200 || response.statusCode() >= 300)");
        body.addStatement("break");
        body.endControlFlow();
        body.beginControlFlow("if (response.body() == null || response.body().isBlank())");
        body.addStatement("break");
        body.endControlFlow();
        body.addStatement("$T json = MAPPER.readValue(response.body(), $T.class)", Object.class, Object.class);

        // Extract next cursor before navigating field path.
        if (parentPaginator != null && parentPaginator.isCursor()) {
            body.add(buildCursorStateUpdate(parentPaginator));
        } else {
            body.addStatement("nextCursor = null");
        }

        if (!fieldPath.isEmpty()) {
            body.add(buildFieldPathNav(fieldPath, false));
        }

        body.addStatement("$T<$T> records", List.class, Object.class);
        body.beginControlFlow("if (json instanceof $T)", List.class);
        body.addStatement("records = ($T<$T>) json", List.class, Object.class);
        body.nextControlFlow("else");
        body.addStatement("records = $T.singletonList(json)", Collections.class);
        body.endControlFlow();

        body.beginControlFlow("for ($T rec : records)", Object.class);
        body.beginControlFlow("if (rec instanceof $T<?, ?> _m)", Map.class);
        body.addStatement("$T val = _m.get($S)", Object.class, parentKey);
        body.beginControlFlow("if (val != null)");
        body.addStatement("keys.add($T.valueOf(val))", String.class);
        body.endControlFlow();
        body.endControlFlow();
        body.endControlFlow();

        body.endControlFlow("while (nextCursor != null && !nextCursor.isEmpty())");
        body.addStatement("return keys");

        return MethodSpec.methodBuilder(methodName)
            .addModifiers(Modifier.PRIVATE)
            .returns(listString)
            .addException(Exception.class)
            .addCode(body.build())
            .build();
    }

    /**
     * Generates the complete poll method for list-cycle streams.
     *
     * <p>A list-cycle stream uses a config field (e.g. {@code tickers}) as a comma-separated list,
     * iterating through items one per {@code poll()} call using the stored {@code ticker_index} offset.
     * No Jinja2 parsing is needed — the pattern is detected structurally from the manifest.
     */
    private MethodSpec buildListCyclePollMethod(
        StreamSpec stream,
        ClassName configClass,
        AuthenticatorSpec auth,
        ParameterizedTypeName listOfSourceRecord,
        String listCycleField
    ) throws CodegenException {
        RequesterSpec requester = stream.getRetriever().getRequester();
        String baseUrl = requester.effectiveBaseUrl();
        String pathPrefix = requester.listCyclePathPrefix();
        Map<String, String> requestParams = requester.getRequestParameters();
        Map<String, String> requestHeaders = requester.getRequestHeaders();
        Set<Integer> successCodes = requester.successHttpCodes();
        List<String> fieldPath = extractFieldPath(stream);
        String streamName = stream.getName();
        String methodName = "poll" + ManifestSpec.toClassName(streamName);
        String itemGetter = "get" + ManifestSpec.toClassName(listCycleField);

        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("final $T streamName = $S", String.class, streamName);
        body.addStatement("$T<$T> result = new $T<>()", List.class, SOURCE_RECORD, ArrayList.class);

        // On first poll() after start, restore position from offset store.
        // Afterwards, tickerIndex is kept in-memory to advance every call without
        // waiting for the 5-second offset flush cycle.
        body.beginControlFlow("if (tickerIndex < 0)");
        body.addStatement(
            "$T<$T, $T> _stored = context.offsetStorageReader().offset($T.of($S, streamName))",
            Map.class, String.class, Object.class, Map.class, "stream");
        body.addStatement("tickerIndex = 0");
        body.beginControlFlow(
            "if (_stored != null && _stored.get($S) instanceof $T _i)", "ticker_index", Number.class);
        body.addStatement("tickerIndex = _i.intValue()");
        body.endControlFlow();
        body.endControlFlow();

        // Split config field into items array
        body.addStatement("$T[] _items = config.$L().split(\",\")", String.class, itemGetter);
        body.beginControlFlow("if (_items.length == 0)");
        body.addStatement("return result");
        body.endControlFlow();
        body.beginControlFlow("if (tickerIndex >= _items.length)");
        body.addStatement("tickerIndex = 0");
        body.endControlFlow();
        body.addStatement("$T _item = _items[tickerIndex].strip()", String.class);
        body.addStatement("int _nextIndex = (tickerIndex + 1 >= _items.length) ? 0 : tickerIndex + 1");

        // Build URL: baseUrl + literal path prefix + URL-encoded current item
        body.addStatement(
            "$T urlBuilder = new $T($S + $T.encode(_item, $T.UTF_8))",
            StringBuilder.class, StringBuilder.class, joinUrl(baseUrl, pathPrefix),
            ClassName.get("java.net", "URLEncoder"),
            ClassName.get("java.nio.charset", "StandardCharsets"));

        // Request parameters: list-cycle ones use _item; normal config templates use getter
        boolean firstParam = !joinUrl(baseUrl, pathPrefix).contains("?");
        Pattern listPat = Pattern.compile("config\\['" + listCycleField + "'\\]\\.split");
        for (Map.Entry<String, String> entry : requestParams.entrySet()) {
            String tmpl = entry.getValue();
            if (tmpl == null) continue;
            String sep = firstParam ? "?" : "&";
            if (listPat.matcher(tmpl).find()) {
                body.addStatement(
                    "urlBuilder.append($S + $T.encode(_item, $T.UTF_8))",
                    sep + entry.getKey() + "=",
                    ClassName.get("java.net", "URLEncoder"),
                    ClassName.get("java.nio.charset", "StandardCharsets"));
            } else if (tmpl.contains("{{") || tmpl.contains("{%")) {
                body.addStatement(
                    "urlBuilder.append($S + $T.encode($T.valueOf($L), $T.UTF_8))",
                    sep + entry.getKey() + "=",
                    ClassName.get("java.net", "URLEncoder"),
                    ClassName.get(String.class), interpolateTemplateWithStream(tmpl),
                    ClassName.get("java.nio.charset", "StandardCharsets"));
            } else {
                // Literal value — URL-encode at codegen time and emit as a constant.
                String encoded = URLEncoder.encode(tmpl, StandardCharsets.UTF_8);
                body.addStatement("urlBuilder.append($S)", sep + entry.getKey() + "=" + encoded);
            }
            firstParam = false;
        }

        // Build HTTP request with optional auth, custom headers and method (GET/POST/PUT)
        body.beginControlFlow("try");
        String lcHttpMethod = requester.getHttpMethod();
        boolean lcJsonBody = needsJsonBody(requester, null);
        boolean lcDataBody = needsDataBody(requester, null);
        String lcBodyVar = null;
        if (lcJsonBody || lcDataBody) {
            try {
                lcBodyVar = buildBodyPreamble(body, requester, null);
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new CodegenException("Failed to serialize request_body_json: " + e.getMessage());
            }
        }
        if (auth != null && auth.isOAuth()) {
            body.addStatement("$T accessToken = refreshAccessToken()", String.class);
        } else if (auth != null && auth.isJwt()) {
            body.addStatement("$T jwtToken = buildJwt()", String.class);
        }
        StringBuilder reqFmt = new StringBuilder(
            "$T request = $T.newBuilder()\n        .uri($T.create(normalizeUrl(urlBuilder.toString().trim().replace(\" \", \"%20\"))))");
        List<Object> reqArgs = new ArrayList<>();
        reqArgs.add(HTTP_REQUEST);
        reqArgs.add(HTTP_REQUEST);
        reqArgs.add(URI_CLASS);
        boolean lcHasExplicitCt = requestHeaders.keySet().stream()
            .anyMatch(k -> k.equalsIgnoreCase("Content-Type"));
        boolean lcHasExplicitUa = requestHeaders.keySet().stream()
            .anyMatch(k -> k.equalsIgnoreCase("User-Agent"));
        boolean lcHasExplicitAccept = requestHeaders.keySet().stream()
            .anyMatch(k -> k.equalsIgnoreCase("Accept"));
        if (!lcHasExplicitUa) {
            reqFmt.append("\n        .header(\"User-Agent\", \"kafka-connect-airbyte/1.0\")");
        }
        if (!lcHasExplicitAccept) {
            reqFmt.append("\n        .header(\"Accept\", \"application/json\")");
        }
        if (lcBodyVar != null && !lcHasExplicitCt) {
            if (lcJsonBody) {
                reqFmt.append("\n        .header(\"Content-Type\", \"application/json\")");
            } else if (lcDataBody) {
                reqFmt.append("\n        .header(\"Content-Type\", \"application/x-www-form-urlencoded\")");
            }
        }
        if (auth == null || auth.isNoAuth() || (auth.isApiKey() && isApiKeyQueryParam(auth))) {
            // no auth header
        } else if (auth.isBearer()) {
            reqFmt.append("\n        .header(\"Authorization\", \"Bearer \" + $L)");
            reqArgs.add(interpolateTemplate(auth.getApiToken()));
        } else if (auth.isApiKey()) {
            reqFmt.append("\n        .header($S, $L)");
            reqArgs.add(resolveApiKeyHeaderName(auth));
            reqArgs.add(interpolateTemplate(auth.getApiToken()));
        } else if (auth.isBasicHttp()) {
            reqFmt.append("\n        .header(\"Authorization\", \"Basic \" + cachedCredentials)");
        } else if (auth.isOAuth()) {
            reqFmt.append("\n        .header(\"Authorization\", \"Bearer \" + accessToken)");
        } else if (auth.isSessionToken()) {
            reqFmt.append("\n        .header(\"Authorization\", \"Bearer \" + cachedSessionToken)");
        } else if (auth.isLegacySessionToken()) {
            reqFmt.append("\n        .header($S, cachedLegacyToken)");
            reqArgs.add(auth.getHeader());
        } else if (auth.isJwt()) {
            reqFmt.append("\n        .header(\"Authorization\", \"$L \" + jwtToken)");
            reqArgs.add(auth.getHeaderPrefix());
        }
        for (Map.Entry<String, String> h : requestHeaders.entrySet()) {
            reqFmt.append("\n        .header($S, $L)");
            reqArgs.add(h.getKey());
            reqArgs.add(interpolateTemplate(h.getValue()));
        }
        appendHttpMethod(reqFmt, reqArgs, lcHttpMethod, lcJsonBody, lcDataBody, lcBodyVar);
        reqFmt.append("\n        .build()");
        body.addStatement(reqFmt.toString(), reqArgs.toArray());

        // Fetch with retry
        emitSendWithRetryCall(body, "response", "request", requester.getErrorHandler());

        // HTTP codes treated as SUCCESS (e.g. 403) → return empty record with advanced index
        for (int code : successCodes) {
            body.beginControlFlow("if (response.statusCode() == $L)", code);
            body.addStatement(
                "result.add(new $T($T.of($S, streamName), $T.of($S, _nextIndex), streamName, $T.STRING_SCHEMA, $S))",
                SOURCE_RECORD, Map.class, "stream", Map.class, "ticker_index", SCHEMA, "{}");
            body.addStatement("tickerIndex = _nextIndex");
            body.addStatement("return result");
            body.endControlFlow();
        }

        body.beginControlFlow("if (response.statusCode() < 200 || response.statusCode() >= 300)");
        body.addStatement(
            "throw new $T(\"HTTP \" + response.statusCode() + \" from \" + urlBuilder)", CONNECT_EXCEPTION);
        body.endControlFlow();
        body.beginControlFlow("if (response.body() == null || response.body().isBlank())");
        body.addStatement("return result");
        body.endControlFlow();
        emitJsonInit(body, stream);

        if (isCustomExtractor(stream)) {
            body.add(buildCustomExtractorNavBlock(stream.getRetriever().getRecordSelector().getExtractor().getClassName()));
        } else if (!fieldPath.isEmpty()) {
            body.add(buildFieldPathNav(fieldPath, true));
        }

        body.addStatement("$T<$T> records", List.class, Object.class);
        body.beginControlFlow("if (json instanceof $T)", List.class);
        body.addStatement("records = ($T<$T>) json", List.class, Object.class);
        body.nextControlFlow("else");
        body.addStatement("records = $T.singletonList(json)", Collections.class);
        body.endControlFlow();

        body.beginControlFlow("for ($T record : records)", Object.class);
        body.add(buildPipelineEmitBlock(pipelineFieldName(streamName),
            CodeBlock.of("$T.of($S, _nextIndex)", Map.class, "ticker_index")));
        body.endControlFlow();
        body.addStatement("tickerIndex = _nextIndex");
        body.addStatement("return result");

        body.nextControlFlow("catch ($T e)", InterruptedException.class);
        body.addStatement("$T.currentThread().interrupt()", Thread.class);
        body.addStatement("throw new $T(\"Interrupted while polling \" + streamName, e)", CONNECT_EXCEPTION);
        body.nextControlFlow("catch ($T e)", Exception.class);
        body.addStatement("throw new $T(\"Failed to poll \" + streamName, e)", CONNECT_EXCEPTION);
        body.endControlFlow();

        return MethodSpec.methodBuilder(methodName)
            .addModifiers(Modifier.PRIVATE)
            .returns(listOfSourceRecord)
            .addException(InterruptedException.class)
            .addCode(body.build())
            .build();
    }

    /**
     * Emits the DatetimeBasedCursor initialisation block.
     * On first poll, restores the cursor from the offset store (if present) or
     * falls back to the config start date.
     */
    private CodeBlock buildIncrementalInit(String streamName, IncrementalSyncSpec sync) {
        String cursorVar = cursorFieldName(streamName);
        CodeBlock.Builder b = CodeBlock.builder();
        b.beginControlFlow("if ($L == null)", cursorVar);
        // Use a distinct variable name to avoid collision with _stored in buildPaginationInit
        // when both pagination and incremental sync are active on the same stream.
        b.addStatement(
            "$T<$T, $T> _incStored = context.offsetStorageReader().offset($T.of($S, streamName))",
            Map.class, String.class, Object.class, Map.class, "stream");
        b.beginControlFlow(
            "if (_incStored != null && _incStored.get($S) instanceof $T _s && !_s.isEmpty())", "cursor", String.class);
        b.addStatement("$L = _s", cursorVar);
        b.nextControlFlow("else");
        IncrementalSyncSpec.DatetimeSpec startDt = sync.getStartDatetime();
        if (startDt != null && startDt.getDatetime() != null) {
            String startFmt = startDt.getDatetimeFormat();
            String cursorFmt = sync.getDatetimeFormat();
            if (startFmt != null && !startFmt.equals(cursorFmt)) {
                // start_datetime uses a different format than the cursor (e.g. date-only vs datetime).
                // Parse with the start_datetime format, then reformat to the cursor format so all
                // downstream DatetimeWindowHelper calls use a consistent format string.
                b.addStatement(
                    "$L = $T.formatDate($T.parseDate($L, $S), $S)",
                    cursorVar, DATETIME_WINDOW_HELPER, DATETIME_WINDOW_HELPER,
                    interpolateTemplate(startDt.getDatetime()), startFmt, cursorFmt);
            } else {
                b.addStatement("$L = $L", cursorVar, interpolateTemplate(startDt.getDatetime()));
            }
        } else {
            b.addStatement("$L = $S", cursorVar, "0");
        }
        b.endControlFlow();
        b.endControlFlow();
        return b.build();
    }

    /**
     * Scans {@code records} for the max value of {@code jsonCursorField} and advances
     * the in-memory {@code cursorVarName} field. Must run before SourceRecords are emitted
     * so that every record's offset already reflects the most advanced position in the batch.
     */
    private CodeBlock buildIncrementalCursorUpdate(String cursorVarName, String jsonCursorField) {
        CodeBlock.Builder b = CodeBlock.builder();
        b.beginControlFlow("for ($T _rec : records)", Object.class);
        b.beginControlFlow("if (_rec instanceof $T<?, ?> _m)", Map.class);
        b.addStatement("$T _cv = _m.get($S)", Object.class, jsonCursorField);
        b.beginControlFlow("if (_cv != null)");
        b.addStatement("$T _cvStr = $T.valueOf(_cv)", String.class, String.class);
        b.beginControlFlow("if ($L == null || _cvStr.compareTo($L) > 0)", cursorVarName, cursorVarName);
        b.addStatement("$L = _cvStr", cursorVarName);
        b.endControlFlow();
        b.endControlFlow();
        b.endControlFlow();
        b.endControlFlow();
        return b.build();
    }

    private CodeBlock buildPaginationInit(PaginatorSpec paginator, boolean hasIncrementalSync) {
        CodeBlock.Builder b = CodeBlock.builder();
        b.addStatement("$T<$T> result = new $T<>()", List.class, SOURCE_RECORD, ArrayList.class);
        // Read stored offset so restarts resume from the last committed position.
        b.addStatement(
            "$T<$T, $T> _stored = context.offsetStorageReader().offset($T.of($S, streamName))",
            Map.class, String.class, Object.class, Map.class, "stream");
        if (paginator.isCursor()) {
            // When IncrementalSync is also present, its datetime value owns "cursor".
            // Use "api_cursor" as the key for the API pagination cursor to avoid collision.
            String apiCursorKey = hasIncrementalSync ? "api_cursor" : "cursor";
            b.addStatement("$T nextCursor = null", String.class);
            b.beginControlFlow("if (_stored != null && _stored.get($S) instanceof $T _c && !_c.isEmpty())",
                apiCursorKey, String.class);
            b.addStatement("nextCursor = _c");
            b.endControlFlow();
            if (isRequestPath(paginator)) {
                b.addStatement("$T url = null", String.class);
            }
            // No loop — framework calls poll() in a loop; each call fetches one page.
        } else if (paginator.isPageIncrement()) {
            int start = paginator.getPaginationStrategy() != null
                ? paginator.getPaginationStrategy().getStartFromPage() : 1;
            b.addStatement("final int startPage = $L", start);
            b.addStatement("int page = startPage");
            b.beginControlFlow(
                "if (_stored != null && _stored.get($S) instanceof $T _p)", "page", Number.class);
            b.addStatement("page = _p.intValue()");
            b.endControlFlow();
            b.addStatement("final int pageLimit = $L", paginator.pageSize());
        } else if (paginator.isOffsetIncrement()) {
            b.addStatement("int offset = 0");
            b.beginControlFlow(
                "if (_stored != null && _stored.get($S) instanceof $T _o)", "offset", Number.class);
            b.addStatement("offset = _o.intValue()");
            b.endControlFlow();
            b.addStatement("final int pageLimit = $L", paginator.pageSize());
        }
        return b.build();
    }

    private boolean isRequestPath(PaginatorSpec paginator) {
        return paginator != null && paginator.isCursor()
            && paginator.getPageTokenOption() != null
            && paginator.getPageTokenOption().isRequestPath();
    }

    private CodeBlock buildUrlBlock(
        String baseUrl, String path,
        Map<String, String> requestParams,
        PaginatorSpec paginator,
        boolean hasPagination,
        AuthenticatorSpec auth,
        IncrementalSyncSpec incrementalSync,
        String cursorVarName,
        StreamSpec stream
    ) {
        CodeBlock.Builder b = CodeBlock.builder();

        if (isRequestPath(paginator)) {
            String initialUrl = joinUrl(baseUrl, path);
            // Append literal request_parameters to the first-page URL; cursor pages carry them already.
            boolean hasQ = initialUrl.contains("?");
            StringBuilder init = new StringBuilder(initialUrl);
            for (Map.Entry<String, String> entry : requestParams.entrySet()) {
                String tmpl = entry.getValue();
                if (tmpl == null || tmpl.contains("{{") || tmpl.contains("{%")) continue;
                String encoded = URLEncoder.encode(tmpl, StandardCharsets.UTF_8);
                init.append(hasQ ? "&" : "?").append(entry.getKey()).append("=").append(encoded);
                hasQ = true;
            }
            initialUrl = init.toString();
            if (!initialUrl.contains("{{") && !initialUrl.contains("{%")) {
                b.addStatement("url = (nextCursor != null) ? nextCursor : $S", initialUrl);
            } else {
                b.addStatement("url = (nextCursor != null) ? nextCursor : $L",
                    interpolateTemplate(initialUrl));
            }
            return b.build();
        }

        addInitialUrlStatement(b, baseUrl, path);

        // Track whether the URL already has a '?' — paths like "/top-headlines?country=us"
        // already contain a query string, so subsequent params must use '&' not '?'.
        boolean pathHasQuery = joinUrl(baseUrl, path).contains("?");

        List<String> paramKeys = new ArrayList<>();
        for (Map.Entry<String, String> entry : requestParams.entrySet()) {
            String tmpl = entry.getValue();
            if (tmpl == null) continue;
            paramKeys.add(entry.getKey());
            boolean firstOfGroup = paramKeys.size() == 1;
            String sep = (pathHasQuery || !firstOfGroup) ? "&" : "?";
            if (tmpl.contains("{{") || tmpl.contains("{%")) {
                // When the template references stream_slice and we are in a window-sliced
                // stream (step != null), inject stream_slice.start_time / end_time into the
                // Jinja context. Airbyte CDK populates stream_slice in DatetimeBasedCursor.
                boolean usesSlice = tmpl.contains("stream_slice");
                boolean isWindowed = incrementalSync != null && incrementalSync.hasStep()
                    && cursorVarName != null;
                String renderExpr = (usesSlice && isWindowed)
                    ? interpolateTemplateWithStreamSlice(tmpl, cursorVarName, "_windowEnd")
                    : interpolateTemplateWithStream(tmpl);
                b.addStatement("urlBuilder.append($S + $T.encode($T.valueOf($L).trim(), $T.UTF_8))",
                    sep + entry.getKey() + "=",
                    ClassName.get("java.net", "URLEncoder"),
                    ClassName.get(String.class), renderExpr,
                    ClassName.get("java.nio.charset", "StandardCharsets"));
            } else {
                // Literal value — URL-encode at codegen time, emit as a string constant.
                // Mirrors Airbyte's InterpolatedRequestOptionsProvider, which passes literal
                // and templated values through the same interpolation pipeline.
                String encoded = URLEncoder.encode(tmpl, StandardCharsets.UTF_8);
                b.addStatement("urlBuilder.append($S)", sep + entry.getKey() + "=" + encoded);
            }
        }

        boolean hasParams = pathHasQuery || !paramKeys.isEmpty();

        // ApiKey injected as query parameter (inject_into: request_parameter)
        if (isApiKeyQueryParam(auth)) {
            String sep = hasParams ? "&" : "?";
            String fieldName = auth.getInjectInto().getFieldName();
            b.addStatement("urlBuilder.append($S + $L)", sep + fieldName + "=",
                interpolateTemplate(auth.getApiToken()));
            hasParams = true;
        }

        // DatetimeBasedCursor: inject start / end date range as query params.
        // _windowEnd (for step-based cursors) is declared before this block by the caller.
        if (incrementalSync != null && incrementalSync.isDatetimeBased()) {
            hasParams = appendIncrementalSyncParams(b, incrementalSync, hasParams, cursorVarName, stream);
        }

        if (!hasPagination || paginator == null) return b.build();
        appendPaginationParams(b, paginator, !hasParams,
            isBodyInjectedJson(paginator) || isBodyInjectedData(paginator));
        return b.build();
    }

    /**
     * Initializes {@code urlBuilder} for the stream poll method.
     * When the base URL or path contains a Jinja template, the initializer is built via
     * {@link JinjaSnippets#interpolateTemplate} so the value is rendered through
     * {@code JinjaRenderer.render(...)} at runtime — same path used for auth headers and cursors.
     */
    private static final java.util.regex.Pattern CONFIG_KEY_IN_PATH =
        java.util.regex.Pattern.compile("\\{\\{\\s*config\\[(['\"])([^'\"]+)\\1\\]\\s*\\}\\}");

    /**
     * Emits an early-return guard for each optional config key referenced in the URL path.
     * When an optional config key is empty/null (the user didn't provide it), the stream
     * cannot form a valid URL, so the poll method returns immediately with an empty list.
     *
     * <p>Required config keys are skipped — if they are absent the connector would have
     * already failed validation. Standard cursor/auth fields ({@code start_date},
     * {@code api_key}, etc.) need no guard because they are always validated at connect time.
     *
     * <p>Airbyte Python CDK: stream skips silently when a required config value is not set;
     * we mirror this by returning an empty record list.
     */
    private void emitOptionalPathConfigGuards(CodeBlock.Builder b, String path,
        ClassName listType, ClassName recordType) {
        if (path == null || !path.contains("{{")) return;
        java.util.regex.Matcher m = CONFIG_KEY_IN_PATH.matcher(path);
        java.util.Set<String> seen = new java.util.LinkedHashSet<>();
        while (m.find()) {
            String key = m.group(2);
            // Emit guard only for keys present in the spec but NOT required.
            if (!currentSpecRequiredKeys.contains(key) && currentSpecPropKeys.contains(key)) {
                seen.add(key);
            }
        }
        for (String key : seen) {
            b.beginControlFlow(
                "if ($T.valueOf(config.originals().getOrDefault($S, \"\")).isEmpty())",
                ClassName.get(String.class), key)
                .addStatement("return new $T<>()", ClassName.get("java.util", "ArrayList"))
                .endControlFlow();
        }
    }

    private void addInitialUrlStatement(CodeBlock.Builder b, String baseUrl, String path) {
        String combined = joinUrl(baseUrl, path);
        if (!combined.contains("{{") && !combined.contains("{%")) {
            b.addStatement("$T urlBuilder = new $T($S)", StringBuilder.class, StringBuilder.class,
                combined);
            return;
        }
        b.addStatement("$T urlBuilder = new $T($L)",
            StringBuilder.class, StringBuilder.class, interpolateTemplate(combined));
    }

    private static boolean containsJinja(String s) {
        return s != null && (s.contains("{{") || s.contains("{%"));
    }

    /**
     * Emits a {@code StringBuilder urlBuilder = new StringBuilder(<before> + URLEncoder.encode(_partitionKey,UTF_8) + <after>)}
     * declaration. {@code before} and {@code after} are URL segments that may contain Jinja
     * templates (e.g. {@code https://{{ config['data_center'] }}.api.mailchimp.com/3.0/lists/})
     * — when they do, they're routed through {@code render(...)} so runtime config values
     * (apikey datacenter suffixes, subdomains) get substituted.
     */
    private void emitSubstreamUrlBuilderDecl(CodeBlock.Builder b, String before, String after) {
        if (!containsJinja(before) && !containsJinja(after)) {
            b.addStatement(
                "$T urlBuilder = new $T($S + $T.encode(_partitionKey, $T.UTF_8) + $S)",
                StringBuilder.class, StringBuilder.class,
                before,
                ClassName.get("java.net", "URLEncoder"),
                ClassName.get("java.nio.charset", "StandardCharsets"),
                after);
            return;
        }
        b.addStatement(
            "$T urlBuilder = new $T($L + $T.encode(_partitionKey, $T.UTF_8) + $L)",
            StringBuilder.class, StringBuilder.class,
            containsJinja(before) ? interpolateTemplate(before) : CodeBlock.of("$S", before),
            ClassName.get("java.net", "URLEncoder"),
            ClassName.get("java.nio.charset", "StandardCharsets"),
            containsJinja(after) ? interpolateTemplate(after) : CodeBlock.of("$S", after));
    }

    /**
     * Like {@link #emitSubstreamUrlBuilderDecl} but emits a {@code String <name> = ...} for the
     * RequestPath paginator path where the result is later guarded by a {@code nextCursor !=
     * null} check.
     */
    private void emitSubstreamBaseUrlDecl(CodeBlock.Builder b, String varName, String before, String after) {
        if (!containsJinja(before) && !containsJinja(after)) {
            b.addStatement(
                "$T $L = $S + $T.encode(_partitionKey, $T.UTF_8) + $S",
                String.class, varName,
                before,
                ClassName.get("java.net", "URLEncoder"),
                ClassName.get("java.nio.charset", "StandardCharsets"),
                after);
            return;
        }
        b.addStatement(
            "$T $L = $L + $T.encode(_partitionKey, $T.UTF_8) + $L",
            String.class, varName,
            containsJinja(before) ? interpolateTemplate(before) : CodeBlock.of("$S", before),
            ClassName.get("java.net", "URLEncoder"),
            ClassName.get("java.nio.charset", "StandardCharsets"),
            containsJinja(after) ? interpolateTemplate(after) : CodeBlock.of("$S", after));
    }

    /**
     * Appends one URL segment (base or path) to the format/args buffers used by callers
     * that interleave URL segments with non-template Java expressions (e.g. URL-encoded
     * loop variables in substream paths). Templated segments are emitted via
     * {@link JinjaSnippets#interpolateTemplate} so they go through {@code render(...)}
     * at runtime; literal segments collapse to a string literal.
     *
     * @return the new value of {@code needsPlus} after appending this segment
     */
    // Joins base URL and path: deduplicates when both have the boundary, adds '/' when neither does.
    // When path is a Jinja expression that renders to a leading '/', the resulting '//' is
    // collapsed at runtime by normalizeUrl() (emitted in every URI.create() call site).
    private static String joinUrl(String base, String path) {
        base = stripOuterQuotes(base);
        path = stripOuterQuotes(path);
        if (base.endsWith("/") && path.startsWith("/")) {
            return base + path.substring(1);  // deduplicate
        }
        if (!base.isEmpty() && !path.isEmpty() && !base.endsWith("/") && !path.startsWith("/")) {
            return base + "/" + path;  // add missing separator
        }
        return base + path;
    }

    /**
     * Strips outer matching single/double quotes from a URL component template.
     *
     * <p>Mirrors Airbyte CDK behaviour: {@code InterpolatedString.eval} runs
     * {@code ast.literal_eval} on each rendered component (url_base, path) which
     * strips literal-quote wrappers like {@code "https://..."}. We do the strip
     * at template level here — before concatenating url_base + path — because our
     * codegen merges both into a single Jinja template before rendering, so the
     * post-render {@code stripOuterQuotes} in JinjaRenderer cannot see them once
     * the path is appended.</p>
     *
     * <p>Example: tiktok-marketing's url_base is the literal string
     * {@code "https://{{ ... }}.tiktok.com/open_api/v1.3/"} — outer {@code "}
     * chars are part of the YAML value and Airbyte strips them; we must too.</p>
     */
    private static String stripOuterQuotes(String s) {
        if (s == null || s.length() < 2) return s;
        char first = s.charAt(0);
        char last = s.charAt(s.length() - 1);
        if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private boolean appendUrlSegment(
        StringBuilder fmt, List<Object> fmtArgs, String segment, boolean needsPlus
    ) {
        if (segment.isEmpty()) return needsPlus;
        if (needsPlus) fmt.append(" + ");
        if (!segment.contains("{{") && !segment.contains("{%")) {
            fmt.append("$S");
            fmtArgs.add(segment);
        } else {
            fmt.append("$L");
            fmtArgs.add(interpolateTemplate(segment));
        }
        return true;
    }

    private boolean isApiKeyQueryParam(AuthenticatorSpec auth) {
        return auth != null && auth.isApiKey()
            && auth.getInjectInto() != null
            && "request_parameter".equalsIgnoreCase(auth.getInjectInto().getInjectInto());
    }

    /**
     * Appends {@code since} and {@code until} query parameters for DatetimeBasedCursor.
     * When {@code cursorVarName} is provided, the in-memory cursor variable is used for
     * {@code since} instead of the config start date, enabling incremental advancement.
     * The {@code until} value is always the current epoch-second timestamp.
     */
    private boolean appendIncrementalSyncParams(
        CodeBlock.Builder b, IncrementalSyncSpec sync, boolean hasParams, String cursorVarName,
        StreamSpec stream
    ) {
        IncrementalSyncSpec.TimeOptionSpec startOpt = sync.getStartTimeOption();
        if (startOpt != null && startOpt.getFieldName() != null) {
            String sep = hasParams ? "&" : "?";
            String startField = expandStreamParams(startOpt.getFieldName(), stream);
            if (cursorVarName != null) {
                // Use the in-memory cursor variable (epoch seconds or ISO string).
                b.addStatement("urlBuilder.append($S + $T.encode($L, $T.UTF_8))",
                    sep + startField + "=",
                    ClassName.get("java.net", "URLEncoder"), cursorVarName,
                    ClassName.get("java.nio.charset", "StandardCharsets"));
            } else {
                IncrementalSyncSpec.DatetimeSpec startDt = sync.getStartDatetime();
                if (startDt != null && startDt.getDatetime() != null) {
                    b.addStatement("urlBuilder.append($S + $T.encode($T.valueOf($L), $T.UTF_8))",
                        sep + startField + "=",
                        ClassName.get("java.net", "URLEncoder"),
                        ClassName.get(String.class), interpolateTemplate(startDt.getDatetime()),
                        ClassName.get("java.nio.charset", "StandardCharsets"));
                }
            }
            hasParams = true;
        }
        IncrementalSyncSpec.TimeOptionSpec endOpt = sync.getEndTimeOption();
        if (endOpt != null && endOpt.getFieldName() != null) {
            String sep = hasParams ? "&" : "?";
            String endField = expandStreamParams(endOpt.getFieldName(), stream);
            if (sync.hasStep()) {
                // Window slicing — use the precomputed _windowEnd (already URL-safe ISO string).
                b.addStatement("urlBuilder.append($S + $T.encode(_windowEnd, $T.UTF_8))",
                    sep + endField + "=",
                    ClassName.get("java.net", "URLEncoder"),
                    ClassName.get("java.nio.charset", "StandardCharsets"));
            } else {
                IncrementalSyncSpec.DatetimeSpec endDt = sync.getEndDatetime();
                if (endDt != null && endDt.getDatetime() != null) {
                    // end_datetime has a Jinja expression (e.g. now_utc().strftime(...)).
                    // Render it at runtime, then reformat to the cursor's datetime_format.
                    String parseFmt = "%Y-%m-%dT%H:%M:%SZ";
                    String cursorFmt = sync.getDatetimeFormat() != null
                        ? sync.getDatetimeFormat() : parseFmt;
                    b.addStatement(
                        "urlBuilder.append($S + $T.encode($T.formatDate($T.parseDate(render($L, jinjaCtx()), $S), $S), $T.UTF_8))",
                        sep + endField + "=",
                        ClassName.get("java.net", "URLEncoder"),
                        DATETIME_WINDOW_HELPER, DATETIME_WINDOW_HELPER,
                        interpolateTemplate(endDt.getDatetime()),
                        parseFmt, cursorFmt,
                        ClassName.get("java.nio.charset", "StandardCharsets"));
                } else {
                    // Epoch seconds fallback — some APIs (e.g. Delighted) expect integer timestamps.
                    b.addStatement("urlBuilder.append($S + $T.valueOf($T.currentTimeMillis() / 1000))",
                        sep + endField + "=",
                        String.class, System.class);
                }
            }
            hasParams = true;
        }
        return hasParams;
    }

    /**
     * Emits {@code String _windowEnd = DatetimeWindowHelper.computeWindowEnd(cursor, fmt, step);}.
     * Immediately follows with {@code if (_windowEnd == null) return result;} so the poll method
     * exits early when the sync has caught up to the current time.
     *
     * <p>The step value is Jinja-interpolated when it contains a template expression.</p>
     */
    private void emitWindowEndComputation(
        CodeBlock.Builder b, IncrementalSyncSpec sync, String cursorVarName
    ) {
        String fmt = sync.getDatetimeFormat();
        String step = sync.getStep();
        boolean stepIsTemplate = step != null && (step.contains("{{") || step.contains("{%"));
        if (stepIsTemplate) {
            b.addStatement("$T _windowEnd = $T.computeWindowEnd($L, $S, $L)",
                String.class, DATETIME_WINDOW_HELPER, cursorVarName, fmt, interpolateTemplate(step));
        } else {
            b.addStatement("$T _windowEnd = $T.computeWindowEnd($L, $S, $S)",
                String.class, DATETIME_WINDOW_HELPER, cursorVarName, fmt, step);
        }
        b.beginControlFlow("if (_windowEnd == null)");
        b.addStatement("return result");
        b.endControlFlow();
    }

    /**
     * Emits cursor advancement for window-sliced incremental sync:
     * {@code cursorVar = DatetimeWindowHelper.advanceCursor(_windowEnd, fmt, granularity);}.
     * Advances past the window end by the cursor_granularity, then persists it.
     */
    private void emitWindowCursorAdvance(
        CodeBlock.Builder b, IncrementalSyncSpec sync, String cursorVarName
    ) {
        String fmt = sync.getDatetimeFormat();
        String gran = sync.getCursorGranularity();
        b.addStatement("$L = $T.advanceCursor(_windowEnd, $S, $S)",
            cursorVarName, DATETIME_WINDOW_HELPER, fmt, gran);
    }

    /**
     * Appends page-token / page-number / offset query-string parameters to {@code urlBuilder}.
     * When {@code skipBodyInjected} is {@code true}, parameters whose {@code inject_into} is
     * {@code body_json} or {@code body_data} are omitted here — they will be added to the
     * request body by {@link #buildBodyPreamble} instead.
     */
    private void appendPaginationParams(
        CodeBlock.Builder b, PaginatorSpec paginator, boolean firstParam, boolean skipBodyInjected
    ) {
        if (skipBodyInjected && (isBodyInjectedJson(paginator) || isBodyInjectedData(paginator))) {
            return;
        }
        String sep = firstParam ? "?" : "&";
        if (paginator.isCursor()
                && paginator.getPageTokenOption() != null
                && !paginator.getPageTokenOption().isRequestPath()) {
            b.beginControlFlow("if (nextCursor != null)");
            b.addStatement("urlBuilder.append($S + nextCursor)", sep + paginator.pageParamName() + "=");
            b.endControlFlow();
        } else if (paginator.isPageIncrement()) {
            b.addStatement("urlBuilder.append($S + page)", sep + paginator.pageParamName() + "=");
            b.addStatement(
                "urlBuilder.append($S + $L)", "&" + paginator.sizeParamName() + "=", paginator.pageSize());
        } else if (paginator.isOffsetIncrement()) {
            // Airbyte CDK default: inject_on_first_request=false — skip the offset param on
            // the first request (offset==0) so APIs that reject explicit ?start=0 still work.
            b.beginControlFlow("if (offset > 0)");
            b.addStatement("urlBuilder.append($S + offset)", sep + paginator.pageParamName() + "=");
            b.addStatement("urlBuilder.append($S + pageLimit)", "&" + paginator.sizeParamName() + "=");
            b.nextControlFlow("else");
            b.addStatement("urlBuilder.append($S + pageLimit)", sep + paginator.sizeParamName() + "=");
            b.endControlFlow();
        }
    }

    private CodeBlock buildFetchBlock(PaginatorSpec paginator, StreamSpec stream) {
        String urlRef = isRequestPath(paginator) ? "url" : "urlBuilder";
        CodeBlock.Builder b = CodeBlock.builder();
        emitSendWithRetryCall(b, "response", "request",
            stream.getRetriever().getRequester().getErrorHandler());
        b.beginControlFlow("if (response.statusCode() < 200 || response.statusCode() >= 300)");
        b.addStatement(
            "throw new $T(\"HTTP \" + response.statusCode() + \" from \" + " + urlRef + ")",
            CONNECT_EXCEPTION);
        b.endControlFlow();
        b.beginControlFlow("if (response.body() == null || response.body().isBlank())");
        b.addStatement("return result");
        b.endControlFlow();
        if (isCustomExtractor(stream)) {
            b.addStatement("$T json = null", Object.class);
        } else {
            b.addStatement("$T json = MAPPER.readValue(response.body(), $T.class)", Object.class, Object.class);
        }
        return b.build();
    }

    private boolean isCustomExtractor(StreamSpec stream) {
        if (stream == null || stream.getRetriever() == null) return false;
        RecordSelectorSpec sel = stream.getRetriever().getRecordSelector();
        if (sel == null || sel.getExtractor() == null) return false;
        // Accept: type=CustomRecordExtractor with class_name, OR class_name only (no type).
        boolean hasClassName = sel.getExtractor().getClassName() != null
            && !sel.getExtractor().getClassName().isBlank();
        if (!hasClassName) return false;
        String t = sel.getExtractor().getType();
        return t == null || t.isBlank() || "CustomRecordExtractor".equals(t);
    }

    private CodeBlock buildCustomExtractorNavBlock(String className) {
        CodeBlock.Builder b = CodeBlock.builder();
        // Use extractFromRawBody so the extractor receives the original bytes,
        // enabling non-JSON bodies (e.g., XML for RSS) to be handled correctly.
        b.addStatement(
            "$T<$T<$T, $T>> _extracted = $T.create($S, $T.class, this.config.originalsStrings(), $T.of())"
                + ".extractFromRawBody(response.body())",
            List.class, Map.class, String.class, Object.class,
            CUSTOM_REGISTRY, className, CUSTOM_RECORD_EXTRACTOR, Map.class);
        b.addStatement("json = _extracted");
        return b.build();
    }

    /** Emits {@code Object json = ...;} for the main poll method, bypassing JSON parsing
     *  when a custom extractor is present (it handles raw body parsing itself). */
    private void emitJsonInit(CodeBlock.Builder body, StreamSpec stream) {
        if (isCustomExtractor(stream)) {
            // Placeholder — actual value set by buildCustomExtractorNavBlock via json = _extracted.
            body.addStatement("$T json = null", Object.class);
        } else {
            body.addStatement("$T json = MAPPER.readValue(response.body(), $T.class)", Object.class, Object.class);
        }
    }

    private CodeBlock buildFieldPathNav(List<String> fieldPath, boolean hasPagination) {
        if (fieldPath.isEmpty()) return CodeBlock.of("");
        CodeBlock.Builder b = CodeBlock.builder();
        b.addStatement("$T current = json", Object.class);
        for (String segment : fieldPath) {
            b.beginControlFlow("if (!(current instanceof $T))", Map.class);
            b.addStatement(hasPagination ? "return result" : "return $T.emptyList()", Collections.class);
            b.endControlFlow();
            b.addStatement("current = (($T<?, ?>) current).get($S)", Map.class, segment);
            b.beginControlFlow("if (current == null)");
            b.addStatement(hasPagination ? "return result" : "return $T.emptyList()", Collections.class);
            b.endControlFlow();
        }
        b.addStatement("json = current");
        return b.build();
    }

    private CodeBlock buildNormalizeAndCollect(
        boolean hasPagination, PaginatorSpec paginator, String cursorVarName, String jsonCursorField,
        String pipelineField
    ) {
        return buildNormalizeAndCollect(hasPagination, paginator, cursorVarName, jsonCursorField,
            pipelineField, null);
    }

    private CodeBlock buildNormalizeAndCollect(
        boolean hasPagination, PaginatorSpec paginator, String cursorVarName, String jsonCursorField,
        String pipelineField, IncrementalSyncSpec incrementalSync
    ) {
        CodeBlock.Builder b = CodeBlock.builder();
        b.addStatement("$T<$T> records", List.class, Object.class);
        b.beginControlFlow("if (json instanceof $T)", List.class);
        b.addStatement("records = ($T<$T>) json", List.class, Object.class);
        b.nextControlFlow("else");
        b.addStatement("records = $T.singletonList(json)", Collections.class);
        b.endControlFlow();

        // Compute next page/offset position.
        if (hasPagination) {
            if (paginator.isPageIncrement()) {
                b.addStatement("int nextPage = records.size() < pageLimit ? startPage : page + 1");
            } else if (paginator.isOffsetIncrement()) {
                b.addStatement("int nextOffset = records.size() < pageLimit ? 0 : offset + pageLimit");
            }
        }

        // For DatetimeBasedCursor: advance cursor before emitting so offsets are current.
        // With step: advance to window end (+ granularity); without step: scan records for max.
        // Capture the cursor value BEFORE the update so that record_filter conditions like
        // {{ record['published'] >= stream_interval['start_time'] }} use the window start,
        // not the post-update max cursor value.
        String prevCursorVar = null;
        if (cursorVarName != null && jsonCursorField != null) {
            if (incrementalSync != null && incrementalSync.hasStep()) {
                emitWindowCursorAdvance(b, incrementalSync, cursorVarName);
            } else {
                prevCursorVar = "_prevCursorForSlice";
                b.addStatement("$T $L = $L", String.class, prevCursorVar, cursorVarName);
                b.add(buildIncrementalCursorUpdate(cursorVarName, jsonCursorField));
            }
        }

        CodeBlock positionMap = buildPositionMapCode(paginator, cursorVarName);
        b.beginControlFlow("for ($T record : records)", Object.class);
        b.add(buildPipelineEmitBlock(pipelineField, positionMap, prevCursorVar));
        b.endControlFlow();
        return b.build();
    }

    private CodeBlock buildPipelineEmitBlock(String pipelineField, CodeBlock positionMap) {
        return buildPipelineEmitBlock(pipelineField, positionMap, null);
    }

    private CodeBlock buildPipelineEmitBlock(String pipelineField, CodeBlock positionMap,
                                             String prevCursorVar) {
        CodeBlock.Builder b = CodeBlock.builder();
        ParameterizedTypeName mapStrObj = ParameterizedTypeName.get(
            ClassName.get("java.util", "Map"),
            ClassName.get(String.class), ClassName.get(Object.class));
        ParameterizedTypeName optMapStrObj = ParameterizedTypeName.get(
            ClassName.get("java.util", "Optional"), mapStrObj);

        // Declare value before if/else so it is in scope for result.add() after the block.
        // Use tp-prefixed names to avoid shadowing any outer _ctx/_rec already in scope.
        b.addStatement("$T value", String.class);
        b.beginControlFlow("if (record instanceof $T)", Map.class);
        b.addStatement("@$T($S) $T _tpRec = ($T) record", SuppressWarnings.class, "unchecked",
            mapStrObj, mapStrObj);
        b.addStatement("$T _tpCtx = jinjaCtx(_tpRec)", mapStrObj);
        if (prevCursorVar != null) {
            // Populate stream_interval so record_filter conditions like
            // {{ record['published'] >= stream_interval['start_time'] }} can evaluate correctly.
            // Use the cursor value captured BEFORE the batch update (not the post-update max).
            b.addStatement(
                "_tpCtx.put($S, $T.of($S, $L != null ? $L : \"\", $S, \"\"))",
                "stream_interval", Map.class,
                "start_time", prevCursorVar, prevCursorVar,
                "end_time");
            b.addStatement(
                "_tpCtx.put($S, $T.of($S, $L != null ? $L : \"\", $S, \"\"))",
                "stream_slice", Map.class,
                "start_time", prevCursorVar, prevCursorVar,
                "end_time");
        }
        b.addStatement("$T _tpKept = $L.process(_tpRec, _tpCtx)", optMapStrObj, pipelineField);
        b.beginControlFlow("if (_tpKept.isEmpty())");
        b.addStatement("continue");
        b.endControlFlow();
        b.addStatement("value = MAPPER.writeValueAsString(_tpKept.get())");
        b.nextControlFlow("else");
        b.addStatement("value = MAPPER.writeValueAsString(record)");
        b.endControlFlow();
        b.addStatement(
            "result.add(new $T($T.of(\"stream\", streamName), $L, streamName, $T.STRING_SCHEMA, value))",
            SOURCE_RECORD, Map.class, positionMap, SCHEMA);
        return b.build();
    }

    private CodeBlock buildPositionMapCode(PaginatorSpec paginator, String cursorVarName) {
        if (cursorVarName != null && paginator != null && paginator.isCursor()) {
            // Both IncrementalSync (datetime cursor) and CursorPagination coexist.
            // Use separate keys to avoid the date cursor being sent as an API page cursor on restart.
            return CodeBlock.of(
                "$T.of(\"cursor\", $L != null ? $L : \"\", \"api_cursor\", nextCursor != null ? nextCursor : \"\")",
                Map.class, cursorVarName, cursorVarName);
        }
        if (cursorVarName != null) {
            return CodeBlock.of("$T.of(\"cursor\", $L != null ? $L : \"\")",
                Map.class, cursorVarName, cursorVarName);
        }
        if (paginator != null && paginator.isCursor()) {
            return CodeBlock.of("$T.of(\"cursor\", nextCursor != null ? nextCursor : \"\")", Map.class);
        } else if (paginator != null && paginator.isPageIncrement()) {
            return CodeBlock.of("$T.of(\"page\", nextPage)", Map.class);
        } else if (paginator != null && paginator.isOffsetIncrement()) {
            return CodeBlock.of("$T.of(\"offset\", nextOffset)", Map.class);
        }
        return CodeBlock.of("$T.of(\"position\", 0)", Map.class);
    }

    /**
     * Extracts the next cursor from the raw response JSON (before field-path navigation).
     * Must be called immediately after the HTTP response is parsed so that {@code json}
     * still points to the full response object, not the navigated records array.
     */
    private CodeBlock buildCursorStateUpdate(PaginatorSpec paginator) {
        CodeBlock.Builder b = CodeBlock.builder();
        b.addStatement("nextCursor = null");
        if (isRequestPath(paginator)) {
            List<String> jsonPath = paginator.getPaginationStrategy() != null
                ? paginator.getPaginationStrategy().parseCursorJsonPath()
                : Collections.emptyList();
            if (!jsonPath.isEmpty()) {
                b.addStatement("$T cursorStep = ($T) json", Object.class, Object.class);
                for (String segment : jsonPath) {
                    b.beginControlFlow("if (cursorStep instanceof $T)", Map.class);
                    b.addStatement("cursorStep = (($T<?, ?>) cursorStep).get($S)", Map.class, segment);
                    b.nextControlFlow("else");
                    b.addStatement("cursorStep = null");
                    b.endControlFlow();
                }
                b.addStatement("nextCursor = cursorStep != null ? $T.valueOf(cursorStep) : null", String.class);
            }
        } else {
            List<String> cursorPath = paginator.getPaginationStrategy() != null
                ? paginator.getPaginationStrategy().parseCursorJsonPath()
                : Collections.emptyList();
            if (!cursorPath.isEmpty()) {
                b.addStatement("$T cursorStep = ($T) json", Object.class, Object.class);
                for (String segment : cursorPath) {
                    b.beginControlFlow("if (cursorStep instanceof $T)", Map.class);
                    b.addStatement("cursorStep = (($T<?, ?>) cursorStep).get($S)", Map.class, segment);
                    b.nextControlFlow("else");
                    b.addStatement("cursorStep = null");
                    b.endControlFlow();
                }
                b.addStatement("nextCursor = cursorStep != null ? $T.valueOf(cursorStep) : null", String.class);
            } else {
                b.beginControlFlow("if (json instanceof $T)", Map.class);
                b.addStatement("$T<?, ?> respMap = ($T<?, ?>) json", Map.class, Map.class);
                b.addStatement("$T nextToken = respMap.get(\"next_page_token\")", Object.class);
                b.addStatement("nextCursor = nextToken != null ? $T.valueOf(nextToken) : null", String.class);
                b.endControlFlow();
            }
        }
        return b.build();
    }

    private CodeBlock buildPaginationLoopClose(PaginatorSpec paginator) {
        return CodeBlock.builder().addStatement("return result").build();
    }

    // ── Body injection detection helpers ─────────────────────────────────────

    private static boolean isBodyInjectedJson(PaginatorSpec paginator) {
        if (paginator == null) return false;
        PaginatorSpec.OptionSpec opt = paginator.getPageTokenOption();
        return opt != null && "body_json".equalsIgnoreCase(opt.getInjectInto());
    }

    private static boolean isBodyInjectedData(PaginatorSpec paginator) {
        if (paginator == null) return false;
        PaginatorSpec.OptionSpec opt = paginator.getPageTokenOption();
        return opt != null && "body_data".equalsIgnoreCase(opt.getInjectInto());
    }

    private static boolean needsJsonBody(RequesterSpec requester, PaginatorSpec paginator) {
        return (requester != null && requester.getRequestBodyJson() != null)
            || isBodyInjectedJson(paginator);
    }

    private static boolean needsDataBody(RequesterSpec requester, PaginatorSpec paginator) {
        return (requester != null && requester.getRequestBodyData() != null)
            || isBodyInjectedData(paginator);
    }

    /**
     * Emits a put into a potentially-nested body map using a field_path.
     * Each call uses a unique numeric suffix on temp-variable names so repeated
     * calls with the same key names in the same method body never clash.
     */
    private void emitNestedBodyPut(
        CodeBlock.Builder b,
        String mapVar,
        java.util.List<String> path,
        String valueFmt,
        Object... valueArgs
    ) {
        if (path == null || path.isEmpty()) return;
        if (path.size() == 1) {
            b.addStatement(mapVar + ".put($S, " + valueFmt + ")", prepend(path.get(0), valueArgs));
        } else {
            int seq = bodyPutSeq++;
            String leafKey = path.get(path.size() - 1);
            // Declare temp var for first intermediate level
            String firstKey = path.get(0);
            String firstVar = "_nb" + seq + "_0";
            b.addStatement(
                "@SuppressWarnings(\"unchecked\") $T<String, Object> " + firstVar
                    + " = ($T<String, Object>) " + mapVar + ".computeIfAbsent($S, k -> new $T<>())",
                java.util.Map.class, java.util.Map.class, firstKey,
                ClassName.get("java.util", "LinkedHashMap"));
            String innerMap = firstVar;
            for (int i = 1; i < path.size() - 1; i++) {
                String seg = path.get(i);
                String segVar = "_nb" + seq + "_" + i;
                b.addStatement(
                    "@SuppressWarnings(\"unchecked\") $T<String, Object> " + segVar
                        + " = ($T<String, Object>) " + innerMap + ".computeIfAbsent($S, k -> new $T<>())",
                    java.util.Map.class, java.util.Map.class, seg,
                    ClassName.get("java.util", "LinkedHashMap"));
                innerMap = segVar;
            }
            b.addStatement(innerMap + ".put($S, " + valueFmt + ")", prepend(leafKey, valueArgs));
        }
    }

    private static Object[] prepend(Object first, Object[] rest) {
        Object[] result = new Object[rest.length + 1];
        result[0] = first;
        System.arraycopy(rest, 0, result, 1, rest.length);
        return result;
    }

    private void emitBodyMapPut(CodeBlock.Builder b, String mapVar, String key, Object val)
            throws com.fasterxml.jackson.core.JsonProcessingException {
        if (val instanceof String strVal) {
            String trimmed = strVal.trim();
            boolean looksLikeJsonObj = trimmed.startsWith("{") || trimmed.startsWith("[");
            if (strVal.contains("{{") || strVal.contains("{%")) {
                // Use window-aware context when the template references stream_interval / stream_slice
                // and we are generating code for a windowed (step-based) DatetimeBasedCursor stream.
                boolean needsWindowCtx = currentWindowStartVar != null
                    && (strVal.contains("stream_interval") || strVal.contains("stream_slice"));
                String renderExpr = needsWindowCtx
                    ? interpolateTemplateWithStreamSlice(strVal, currentWindowStartVar, currentWindowEndVar)
                    : interpolateTemplate(strVal);
                if (looksLikeJsonObj) {
                    // Template renders to a JSON object/array — parse at runtime so it embeds correctly.
                    b.addStatement("$L.put($S, MAPPER.readValue($L, $T.class))",
                        mapVar, key, renderExpr, Object.class);
                } else {
                    b.addStatement("$L.put($S, $L)", mapVar, key, renderExpr);
                }
            } else if (looksLikeJsonObj) {
                // Static JSON object/array string — parse at codegen-emit time so it embeds correctly.
                b.addStatement("$L.put($S, MAPPER.readValue($S, $T.class))",
                    mapVar, key, strVal, Object.class);
            } else {
                b.addStatement("$L.put($S, $S)", mapVar, key, strVal);
            }
        } else if (val instanceof Number || val instanceof Boolean) {
            b.addStatement("$L.put($S, $L)", mapVar, key, val);
        } else if (val == null) {
            b.addStatement("$L.put($S, ($T) null)", mapVar, key, Object.class);
        } else {
            String jsonLiteral = CODEGEN_MAPPER.writeValueAsString(val);
            b.addStatement("$L.put($S, MAPPER.readValue($S, $T.class))",
                mapVar, key, jsonLiteral, Object.class);
        }
    }

    /**
     * Emits code to build the POST/PUT request body into variable {@code _bodyStr}.
     * Returns {@code "_bodyStr"} if a body was emitted, {@code null} if no body is needed.
     *
     * <p>Handles:
     * <ul>
     *   <li>{@code request_body_json} — emits a {@code Map<String,Object>} and serializes to JSON.</li>
     *   <li>Paginator {@code inject_into: body_json} — adds page-token to the body map.</li>
     *   <li>{@code request_body_data} raw string — emits the string directly.</li>
     *   <li>{@code request_body_data} form map / paginator {@code inject_into: body_data} —
     *       builds URL-encoded form string.</li>
     * </ul>
     */
    private String buildBodyPreamble(
        CodeBlock.Builder b, RequesterSpec requester, PaginatorSpec paginator
    ) throws com.fasterxml.jackson.core.JsonProcessingException {
        return buildBodyPreamble(b, requester, paginator, "");
    }

    /**
     * @param varSuffix appended to {@code _bodyMap}/{@code _bodyStr} to avoid name collisions
     *                  when this method is called multiple times in the same scope.
     */
    private String buildBodyPreamble(
        CodeBlock.Builder b, RequesterSpec requester, PaginatorSpec paginator, String varSuffix
    ) throws com.fasterxml.jackson.core.JsonProcessingException {
        boolean jsonBody = needsJsonBody(requester, paginator);
        boolean dataBody = !jsonBody && needsDataBody(requester, paginator);

        if (!jsonBody && !dataBody) {
            return null;
        }

        String bodyMapVar = "_bodyMap" + varSuffix;
        String bodyStrVar = "_bodyStr" + varSuffix;

        ParameterizedTypeName mapStrObj = ParameterizedTypeName.get(
            ClassName.get("java.util", "Map"), ClassName.get(String.class), ClassName.get(Object.class));

        if (jsonBody) {
            b.addStatement("$T $L = new $T<>()", mapStrObj, bodyMapVar, ClassName.get("java.util", "LinkedHashMap"));
            if (requester != null && requester.getRequestBodyJson() != null) {
                for (Map.Entry<String, Object> entry : requester.getRequestBodyJson().entrySet()) {
                    emitBodyMapPut(b, bodyMapVar, entry.getKey(), entry.getValue());
                }
            }
            if (isBodyInjectedJson(paginator)) {
                java.util.List<String> tokenPath = paginator.getPageTokenOption().getEffectivePath();
                if (!tokenPath.isEmpty()) {
                    if (paginator.isCursor()) {
                        b.beginControlFlow("if (nextCursor != null)");
                        emitNestedBodyPut(b, bodyMapVar, tokenPath, "nextCursor");
                        b.endControlFlow();
                    } else if (paginator.isOffsetIncrement()) {
                        emitNestedBodyPut(b, bodyMapVar, tokenPath, "$T.valueOf(offset)", String.class);
                    } else if (paginator.isPageIncrement()) {
                        emitNestedBodyPut(b, bodyMapVar, tokenPath, "$T.valueOf(page)", String.class);
                    }
                }
            }
            if (paginator != null && paginator.getPageSizeOption() != null
                    && "body_json".equalsIgnoreCase(paginator.getPageSizeOption().getInjectInto())) {
                java.util.List<String> sizePath = paginator.getPageSizeOption().getEffectivePath();
                if (!sizePath.isEmpty()) {
                    if (paginator.isCursor()) {
                        // For cursor paginators pageLimit is not declared as a variable; use literal.
                        emitNestedBodyPut(b, bodyMapVar, sizePath, "$L", paginator.pageSize());
                    } else {
                        emitNestedBodyPut(b, bodyMapVar, sizePath, "$T.valueOf(pageLimit)", String.class);
                    }
                }
            }
            b.addStatement("$T $L = MAPPER.writeValueAsString($L)", String.class, bodyStrVar, bodyMapVar);
        } else {
            // data body
            RequesterSpec.BodyDataSpec dataSpec = requester != null ? requester.getRequestBodyData() : null;
            if (dataSpec != null && dataSpec.isRaw()) {
                b.addStatement("$T $L = $L", String.class, bodyStrVar, interpolateTemplate(dataSpec.getRawBody()));
            } else {
                b.addStatement("$T _bodyBuilder = new $T()", StringBuilder.class, StringBuilder.class);
                boolean firstField = true;
                if (dataSpec != null && dataSpec.getFormFields() != null) {
                    for (Map.Entry<String, String> e : dataSpec.getFormFields().entrySet()) {
                        String sep = firstField ? "" : "&";
                        b.addStatement("_bodyBuilder.append($S + $T.encode($L, $T.UTF_8))",
                            sep + e.getKey() + "=",
                            ClassName.get("java.net", "URLEncoder"),
                            interpolateTemplate(e.getValue()),
                            STD_CHARSETS);
                        firstField = false;
                    }
                }
                if (isBodyInjectedData(paginator)) {
                    String fieldName = paginator.getPageTokenOption().getFieldName();
                    String sep = firstField ? "" : "&";
                    if (paginator.isCursor()) {
                        b.beginControlFlow("if (nextCursor != null)");
                        b.addStatement("_bodyBuilder.append($S + $T.encode(nextCursor, $T.UTF_8))",
                            sep + fieldName + "=",
                            ClassName.get("java.net", "URLEncoder"),
                            STD_CHARSETS);
                        b.endControlFlow();
                    } else if (paginator.isOffsetIncrement()) {
                        b.addStatement("_bodyBuilder.append($S + $T.valueOf(offset))",
                            sep + fieldName + "=", String.class);
                    } else if (paginator.isPageIncrement()) {
                        b.addStatement("_bodyBuilder.append($S + $T.valueOf(page))",
                            sep + fieldName + "=", String.class);
                    }
                }
                b.addStatement("$T $L = _bodyBuilder.toString()", String.class, bodyStrVar);
            }
        }
        return bodyStrVar;
    }

    /**
     * Appends {@code .POST(publisher)} or {@code .PUT(publisher)} or {@code .GET()} to the
     * given format-string builder. The caller provides an {@code args} list to which any
     * ClassName type tokens are appended.
     */
    private void appendHttpMethod(
        StringBuilder fmt, List<Object> args,
        String httpMethod, boolean jsonBody, boolean dataBody, String bodyVar
    ) {
        boolean isPost = "POST".equalsIgnoreCase(httpMethod);
        boolean isPut  = "PUT".equalsIgnoreCase(httpMethod);

        if ((isPost || isPut) && bodyVar != null) {
            fmt.append(isPost
                ? "\n        .POST($T.ofString($L, $T.UTF_8))"
                : "\n        .PUT($T.ofString($L, $T.UTF_8))");
            args.add(BODY_PUBLISHERS);
            args.add(bodyVar);
            args.add(STD_CHARSETS);
        } else if (isPost) {
            fmt.append("\n        .POST($T.noBody())");
            args.add(BODY_PUBLISHERS);
        } else if (isPut) {
            fmt.append("\n        .PUT($T.noBody())");
            args.add(BODY_PUBLISHERS);
        } else {
            fmt.append("\n        .GET()");
        }
    }

    /**
     * Generates the {@code HttpRequest.newBuilder()...build()} statement with auth headers,
     * custom request headers, and the appropriate HTTP method (GET/POST/PUT) with body.
     *
     * <p>Call {@link #buildBodyPreamble} separately when the method is POST/PUT so the body
     * variables are in scope before this statement runs.</p>
     */
    private void buildRequestStatement(
        CodeBlock.Builder body, AuthenticatorSpec auth, PaginatorSpec paginator,
        RequesterSpec requester
    ) throws CodegenException {
        String httpMethod  = requester != null ? requester.getHttpMethod() : "GET";
        boolean jsonBody   = needsJsonBody(requester, paginator);
        boolean dataBody   = needsDataBody(requester, paginator);
        boolean hasBody    = jsonBody || dataBody;

        // Emit body-building preamble (sets _bodyStr) when applicable.
        String bodyVar = null;
        if (hasBody) {
            try {
                bodyVar = buildBodyPreamble(body, requester, paginator);
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new CodegenException("Failed to serialize request_body_json: " + e.getMessage());
            }
        }

        // Pre-statements for auth mechanisms that need a helper call first.
        if (auth != null && auth.isOAuth()) {
            body.addStatement("$T accessToken = refreshAccessToken()", String.class);
        } else if (auth != null && auth.isJwt()) {
            body.addStatement("$T jwtToken = buildJwt()", String.class);
        }

        StringBuilder fmt = new StringBuilder(
            "$T request = $T.newBuilder()\n        .uri($T.create(normalizeUrl(($L).trim().replace(\" \", \"%20\"))))");
        List<Object> args = new ArrayList<>();
        args.add(HTTP_REQUEST);
        args.add(HTTP_REQUEST);
        args.add(URI_CLASS);
        args.add(urlExpr(auth, paginator));

        // Content-Type header when a body is present — skip if manifest already declares one.
        Map<String, String> reqHeaders = requester != null
            ? requester.getRequestHeaders() : Collections.emptyMap();
        boolean hasExplicitContentType = reqHeaders.keySet().stream()
            .anyMatch(k -> k.equalsIgnoreCase("Content-Type"));
        boolean hasExplicitUserAgent = reqHeaders.keySet().stream()
            .anyMatch(k -> k.equalsIgnoreCase("User-Agent"));
        boolean hasExplicitAccept = reqHeaders.keySet().stream()
            .anyMatch(k -> k.equalsIgnoreCase("Accept"));
        if (!hasExplicitUserAgent) {
            fmt.append("\n        .header(\"User-Agent\", \"kafka-connect-airbyte/1.0\")");
        }
        if (!hasExplicitAccept) {
            fmt.append("\n        .header(\"Accept\", \"application/json\")");
        }
        if (bodyVar != null && !hasExplicitContentType) {
            if (jsonBody) {
                fmt.append("\n        .header(\"Content-Type\", \"application/json\")");
            } else if (dataBody) {
                fmt.append("\n        .header(\"Content-Type\", \"application/x-www-form-urlencoded\")");
            }
        }

        // Auth headers.
        if (auth == null || auth.isNoAuth() || (auth.isApiKey() && isApiKeyQueryParam(auth))) {
            // no auth header
        } else if (auth.isBearer()) {
            fmt.append("\n        .header(\"Authorization\", \"Bearer \" + $L)");
            args.add(interpolateTemplate(auth.getApiToken()));
        } else if (auth.isApiKey()) {
            fmt.append("\n        .header($S, $L)");
            args.add(resolveApiKeyHeaderName(auth));
            args.add(interpolateTemplate(auth.getApiToken()));
        } else if (auth.isBasicHttp()) {
            fmt.append("\n        .header(\"Authorization\", \"Basic \" + cachedCredentials)");
        } else if (auth.isOAuth()) {
            fmt.append("\n        .header(\"Authorization\", \"Bearer \" + accessToken)");
        } else if (auth.isSessionToken()) {
            fmt.append("\n        .header(\"Authorization\", \"Bearer \" + cachedSessionToken)");
        } else if (auth.isLegacySessionToken()) {
            fmt.append("\n        .header($S, cachedLegacyToken)");
            args.add(auth.getHeader());
        } else if (auth.isJwt()) {
            fmt.append("\n        .header(\"Authorization\", \"$L \" + jwtToken)");
            args.add(auth.getHeaderPrefix());
        }

        // Custom request headers from the manifest.
        for (Map.Entry<String, String> h : reqHeaders.entrySet()) {
            fmt.append("\n        .header($S, $L)");
            args.add(h.getKey());
            args.add(interpolateTemplate(h.getValue()));
        }

        // HTTP method.
        appendHttpMethod(fmt, args, httpMethod, jsonBody, dataBody, bodyVar);
        fmt.append("\n        .build()");

        body.addStatement(fmt.toString(), args.toArray());
    }

    /**
     * Returns the URL expression string to use in the request builder.
     * For RequestPath cursor, the cursor itself is the full URL; otherwise use urlBuilder.
     */
    private String urlExpr(AuthenticatorSpec auth, PaginatorSpec paginator) {
        if (paginator != null && paginator.isCursor()
                && paginator.getPageTokenOption() != null
                && paginator.getPageTokenOption().isRequestPath()) {
            return "url";
        }
        return "urlBuilder.toString()";
    }

    private void addAuthHelperMethods(
        TypeSpec.Builder typeBuilder, ClassName configClass, AuthenticatorSpec auth,
        String baseUrl
    ) {
        if (auth == null) return;
        if (auth.isOAuth()) {
            typeBuilder.addMethod(buildRefreshAccessToken(configClass, auth));
        }
        if (auth.isSessionToken()) {
            typeBuilder.addMethod(buildLoginAndCacheSessionToken(configClass, auth));
        }
        if (auth.isLegacySessionToken()) {
            typeBuilder.addMethod(buildLoginAndCacheLegacyToken(configClass, auth, baseUrl));
        }
        if (auth.isJwt()) {
            typeBuilder.addMethod(buildBuildJwt(configClass, auth));
        }
    }

    /**
     * Generates a private {@code refreshAccessToken()} method for OAuth connectors.
     * Caches the token using {@code tokenExpiryMs}; refreshes only when expired.
     */
    private MethodSpec buildRefreshAccessToken(ClassName configClass, AuthenticatorSpec auth) {
        String clientIdExpr     = interpolateTemplate(auth.getClientId());
        String clientSecretExpr = interpolateTemplate(auth.getClientSecret());
        String refreshTokenExpr = interpolateTemplate(auth.getRefreshToken());
        String endpoint = auth.getTokenRefreshEndpoint() != null
            ? interpolateTemplate(auth.getTokenRefreshEndpoint()) : "";

        CodeBlock.Builder body = CodeBlock.builder();
        body.beginControlFlow("if (cachedToken != null && $T.currentTimeMillis() < tokenExpiryMs)", System.class);
        body.addStatement("return cachedToken");
        body.endControlFlow();

        if (auth.isClientCredentials()) {
            StringBuilder extraFields = new StringBuilder();
            if (auth.getRefreshRequestBody() != null) {
                for (Map.Entry<String, String> e : auth.getRefreshRequestBody().entrySet()) {
                    extraFields.append("\n        + \"&").append(e.getKey())
                        .append("=\" + ").append(interpolateTemplate(e.getValue()));
                }
            }
            body.addStatement("$T reqBody = \"grant_type=client_credentials\"\n"
                    + "        + \"&client_id=\" + $L\n"
                    + "        + \"&client_secret=\" + $L"
                    + extraFields,
                String.class, clientIdExpr, clientSecretExpr);
        } else {
            body.addStatement("$T reqBody = \"grant_type=refresh_token\"\n"
                    + "        + \"&client_id=\" + $L\n"
                    + "        + \"&client_secret=\" + $L\n"
                    + "        + \"&refresh_token=\" + $L",
                String.class, clientIdExpr, clientSecretExpr, refreshTokenExpr);
        }
        body.beginControlFlow("try");
        body.addStatement(
            "$T tokenRequest = $T.newBuilder()\n"
                + "        .uri($T.create($L))\n"
                + "        .header(\"Content-Type\", \"application/x-www-form-urlencoded\")\n"
                + "        .POST($T.ofString(reqBody))\n"
                + "        .build()",
            HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, endpoint,
            ClassName.get("java.net.http", "HttpRequest.BodyPublishers")
        );
        body.addStatement(
            "$T<$T> tokenResponse = httpClient.send(tokenRequest, $T.BodyHandlers.ofString())",
            HTTP_RESPONSE, String.class, HTTP_RESPONSE
        );
        body.addStatement(
            "$T rawTokenJson = MAPPER.readValue(tokenResponse.body(), $T.class)",
            Object.class, Object.class
        );
        body.addStatement("$T<?, ?> tokenJson = ($T<?, ?>) rawTokenJson", Map.class, Map.class);
        body.addStatement("$T token = tokenJson.get($S)", Object.class, auth.getAccessTokenName());
        body.beginControlFlow("if (token == null)");
        body.addStatement("throw new $T(\"No access_token in token refresh response\")", CONNECT_EXCEPTION);
        body.endControlFlow();
        body.addStatement("$T expiresInObj = tokenJson.get(\"expires_in\")", Object.class);
        body.addStatement(
            "long expiresIn = expiresInObj != null ? $T.parseLong($T.valueOf(expiresInObj)) : 3600L",
            Long.class, String.class
        );
        body.addStatement(
            "tokenExpiryMs = $T.currentTimeMillis() + (expiresIn - 60L) * 1000L", System.class);
        body.addStatement("cachedToken = $T.valueOf(token)", String.class);
        body.addStatement("return cachedToken");
        body.nextControlFlow("catch ($T e)", InterruptedException.class);
        body.addStatement("$T.currentThread().interrupt()", Thread.class);
        body.addStatement("throw new $T(\"Interrupted during token refresh\", e)", CONNECT_EXCEPTION);
        body.nextControlFlow("catch ($T e)", Exception.class);
        body.addStatement("throw new $T(\"Failed to refresh access token\", e)", CONNECT_EXCEPTION);
        body.endControlFlow();

        return MethodSpec.methodBuilder("refreshAccessToken")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class)
            .addCode(body.build())
            .build();
    }

    /**
     * Generates a private {@code loginAndCacheSessionToken()} method for SessionTokenAuthenticator.
     * POSTs to the login endpoint with Basic auth + JSON body, then navigates session_token_path.
     */
    private MethodSpec buildLoginAndCacheSessionToken(ClassName configClass, AuthenticatorSpec auth) {
        AuthenticatorSpec.LoginRequesterSpec login = auth.getLoginRequester();
        if (login == null) {
            return MethodSpec.methodBuilder("loginAndCacheSessionToken")
                .addModifiers(Modifier.PRIVATE)
                .addException(Exception.class)
                .build();
        }

        // Resolve the login URL — may be a mixed template like "{{ config["host"] }}/api/oauth/v1"
        String loginUrlExpr = interpolateTemplate(login.getUrlBase() + "/" + login.getPath());

        // Determine body style: request_body_json → JSON, request_body_data → form-encoded, else empty
        boolean hasBodyJson = login.getRequestBodyJson() != null && !login.getRequestBodyJson().isEmpty();
        boolean hasBodyData = login.getRequestBodyData() != null && !login.getRequestBodyData().isEmpty();
        boolean hasQueryParams = login.getRequestParameters() != null && !login.getRequestParameters().isEmpty();

        CodeBlock.Builder body = CodeBlock.builder();

        // Build query-param suffix for request_parameters
        if (hasQueryParams) {
            body.addStatement("$T<$T, $T> qpMap = new $T<>()", Map.class, String.class, String.class,
                ClassName.get("java.util", "LinkedHashMap"));
            for (Map.Entry<String, String> e : login.getRequestParameters().entrySet()) {
                body.addStatement("qpMap.put($S, $L)", e.getKey(), interpolateTemplate(e.getValue()));
            }
            body.addStatement("$T qpBuilder = new $T()", StringBuilder.class, StringBuilder.class);
            body.addStatement(
                "qpMap.forEach((k, v) -> qpBuilder.append(qpBuilder.length() == 0 ? '?' : '&').append(k).append('=').append(v))");
        }

        // Build body string
        if (hasBodyJson) {
            body.addStatement("$T<$T, $T> bodyMap = new $T<>()", Map.class, String.class, String.class,
                ClassName.get("java.util", "LinkedHashMap"));
            for (Map.Entry<String, String> e : login.getRequestBodyJson().entrySet()) {
                body.addStatement("bodyMap.put($S, $L)", e.getKey(), interpolateTemplate(e.getValue()));
            }
            body.addStatement("$T loginBody = MAPPER.writeValueAsString(bodyMap)", String.class);
        } else if (hasBodyData) {
            body.addStatement("$T<$T, $T> formMap = new $T<>()", Map.class, String.class, String.class,
                ClassName.get("java.util", "LinkedHashMap"));
            for (Map.Entry<String, String> e : login.getRequestBodyData().entrySet()) {
                body.addStatement("formMap.put($S, $L)", e.getKey(), interpolateTemplate(e.getValue()));
            }
            body.addStatement("$T loginFormSb = new $T()", StringBuilder.class, StringBuilder.class);
            body.addStatement(
                "formMap.forEach((k, v) -> loginFormSb.append(loginFormSb.length() == 0 ? \"\" : \"&\").append(k).append('=').append(v))");
            body.addStatement("$T loginBody = loginFormSb.toString()", String.class);
        } else {
            body.addStatement("$T loginBody = \"\"", String.class);
        }

        // Build the request
        ClassName bodyPublishers = ClassName.get("java.net.http", "HttpRequest.BodyPublishers");
        boolean loginIsGet = "GET".equals(login.getHttpMethod());
        String uriExpr = hasQueryParams
            ? "((" + loginUrlExpr + ") + qpBuilder.toString()).trim().replace(\" \", \"%20\")"
            : "(" + loginUrlExpr + ").trim().replace(\" \", \"%20\")";
        StringBuilder reqBuilder = new StringBuilder(
            "$T loginReq = $T.newBuilder()\n"
                + "        .uri($T.create(" + uriExpr + "))\n");
        if (!loginIsGet) {
            if (hasBodyData) {
                reqBuilder.append("        .header(\"Content-Type\", \"application/x-www-form-urlencoded\")\n");
            } else {
                reqBuilder.append("        .header(\"Content-Type\", \"application/json\")\n");
            }
        }

        List<Object> reqArgs = new ArrayList<>();
        reqArgs.add(HTTP_REQUEST);
        reqArgs.add(HTTP_REQUEST);
        reqArgs.add(URI_CLASS);

        // Emit custom request_headers from login_requester
        if (login.getRequestHeaders() != null) {
            for (Map.Entry<String, String> e : login.getRequestHeaders().entrySet()) {
                if (e.getValue() != null && !e.getValue().isEmpty()) {
                    reqBuilder.append("        .header($S, $L)\n");
                    reqArgs.add(e.getKey());
                    reqArgs.add(interpolateTemplate(e.getValue()));
                }
            }
        }

        // Add auth headers for the inner login_requester authenticator
        AuthenticatorSpec innerAuth = login.getAuthenticator();
        if (innerAuth != null && innerAuth.isBasicHttp()) {
            String userExpr = interpolateTemplate(innerAuth.getUsername());
            String passExpr = interpolateTemplate(innerAuth.getPassword());
            reqBuilder.append("        .header(\"Authorization\", \"Basic \" + $T.getEncoder().encodeToString(\n"
                + "                ($L + \":\" + $L).getBytes($T.UTF_8)))\n");
            reqArgs.add(ClassName.get("java.util", "Base64"));
            reqArgs.add(userExpr);
            reqArgs.add(passExpr);
            reqArgs.add(ClassName.get("java.nio.charset", "StandardCharsets"));
        } else if (innerAuth != null && innerAuth.isApiKey()) {
            String tokenExpr = interpolateTemplate(innerAuth.getApiToken() != null
                ? innerAuth.getApiToken() : "");
            AuthenticatorSpec.InjectIntoSpec inject = innerAuth.getInjectInto();
            String headerName = (inject != null && inject.getFieldName() != null)
                ? inject.getFieldName() : (innerAuth.getHeader() != null ? innerAuth.getHeader() : "x-api-key");
            reqBuilder.append("        .header($S, $L)\n");
            reqArgs.add(headerName);
            reqArgs.add(tokenExpr);
        } else if (innerAuth != null && innerAuth.isBearer()) {
            String tokenExpr = interpolateTemplate(innerAuth.getApiToken() != null
                ? innerAuth.getApiToken() : "");
            reqBuilder.append("        .header(\"Authorization\", \"Bearer \" + $L)\n");
            reqArgs.add(tokenExpr);
        }
        if (loginIsGet) {
            reqBuilder.append("        .GET()\n        .build()");
        } else {
            reqBuilder.append("        .POST($T.ofString(loginBody))\n        .build()");
            reqArgs.add(bodyPublishers);
        }

        body.addStatement(reqBuilder.toString(), reqArgs.toArray());
        body.addStatement(
            "$T<$T> loginResp = httpClient.send(loginReq, $T.BodyHandlers.ofString())",
            HTTP_RESPONSE, String.class, HTTP_RESPONSE);
        body.beginControlFlow("if (loginResp.statusCode() < 200 || loginResp.statusCode() >= 300)");
        body.addStatement(
            "throw new $T(\"Session token login failed: HTTP \" + loginResp.statusCode() + \" \" + loginResp.body().substring(0, Math.min(200, loginResp.body().length())))",
            CONNECT_EXCEPTION);
        body.endControlFlow();
        body.addStatement(
            "$T loginJson = MAPPER.readValue(loginResp.body(), $T.class)", Object.class, Object.class);

        // Navigate session_token_path
        List<String> tokenPath = auth.getSessionTokenPath() == null
            ? Collections.emptyList() : auth.getSessionTokenPath();
        body.addStatement("$T tokenStep = loginJson", Object.class);
        for (String segment : tokenPath) {
            body.beginControlFlow("if (tokenStep instanceof $T)", Map.class);
            body.addStatement("tokenStep = (($T<?, ?>) tokenStep).get($S)", Map.class, segment);
            body.nextControlFlow("else");
            body.addStatement("tokenStep = null");
            body.endControlFlow();
        }
        body.beginControlFlow("if (tokenStep == null)");
        body.addStatement(
            "throw new $T(\"Session token not found at path $L in login response: \" + loginResp.body().substring(0, Math.min(200, loginResp.body().length())))",
            CONNECT_EXCEPTION, String.join(".", tokenPath));
        body.endControlFlow();
        body.addStatement("cachedSessionToken = $T.valueOf(tokenStep)", String.class);

        return MethodSpec.methodBuilder("loginAndCacheSessionToken")
            .addModifiers(Modifier.PRIVATE)
            .addException(Exception.class)
            .addCode(body.build())
            .build();
    }

    /**
     * Generates a private {@code loginAndCacheLegacyToken()} method for LegacySessionTokenAuthenticator.
     * POSTs credentials to the login endpoint, extracts the token key, and caches it.
     */
    private MethodSpec buildLoginAndCacheLegacyToken(
        ClassName configClass, AuthenticatorSpec auth, String baseUrl
    ) {
        String loginUrlExpr = interpolateTemplate(baseUrl + auth.getLoginUrl());

        String userExpr = interpolateTemplate(auth.getUsername());
        String passExpr = interpolateTemplate(auth.getPassword());
        ClassName bodyPublishers = ClassName.get("java.net.http", "HttpRequest.BodyPublishers");

        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("$T loginBody = \"{\\\"username\\\":\\\"\" + $L\n"
            + "        + \"\\\",\\\"password\\\":\\\"\" + $L + \"\\\"}\"",
            String.class, userExpr, passExpr);
        body.addStatement(
            "$T loginReq = $T.newBuilder()\n"
                + "        .uri($T.create(" + loginUrlExpr + "))\n"
                + "        .header(\"Content-Type\", \"application/json\")\n"
                + "        .POST($T.ofString(loginBody))\n"
                + "        .build()",
            HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, bodyPublishers);
        body.addStatement(
            "$T<$T> loginResp = httpClient.send(loginReq, $T.BodyHandlers.ofString())",
            HTTP_RESPONSE, String.class, HTTP_RESPONSE);
        body.addStatement(
            "$T rawJson = MAPPER.readValue(loginResp.body(), $T.class)", Object.class, Object.class);
        body.addStatement("$T tokenVal = (($T<?, ?>) rawJson).get($S)",
            Object.class, Map.class, auth.getSessionTokenResponseKey());
        body.beginControlFlow("if (tokenVal == null)");
        body.addStatement(
            "throw new $T(\"Token key '$L' not found in login response\")",
            CONNECT_EXCEPTION, auth.getSessionTokenResponseKey());
        body.endControlFlow();
        body.addStatement("cachedLegacyToken = $T.valueOf(tokenVal)", String.class);

        return MethodSpec.methodBuilder("loginAndCacheLegacyToken")
            .addModifiers(Modifier.PRIVATE)
            .addException(Exception.class)
            .addCode(body.build())
            .build();
    }

    /**
     * Generates a private {@code buildJwt()} method for JwtAuthenticator.
     * Constructs and signs a JWT using the RSA private key from config (RS256 algorithm).
     */
    private MethodSpec buildBuildJwt(ClassName configClass, AuthenticatorSpec auth) {
        CodeBlock.Builder body = CodeBlock.builder();

        String secretKey = auth.getSecretKey() == null ? "" : auth.getSecretKey();
        Pattern jsonLoads = Pattern.compile("json_loads\\(config\\[['\"]([^'\"]+)['\"]\\]\\)\\[['\"]([^'\"]+)['\"]\\]");
        Matcher jlMatcher = jsonLoads.matcher(secretKey);
        Pattern nestedConfig = Pattern.compile("config\\[['\"]([^'\"]+)['\"]\\]\\[['\"]([^'\"]+)['\"]\\]");
        Matcher ncMatcher = nestedConfig.matcher(secretKey);

        body.beginControlFlow("try");

        if (jlMatcher.find()) {
            String cfgGetter = "get" + ManifestSpec.toClassName(jlMatcher.group(1));
            String jsonKey = jlMatcher.group(2);
            body.addStatement("$T credsMap = ($T<?, ?>) MAPPER.readValue(config.$L(), $T.class)",
                ClassName.get("java.util", "Map"), Map.class, cfgGetter, Object.class);
            body.addStatement("$T privateKeyPem = ($T) credsMap.get($S)", String.class, String.class, jsonKey);
        } else if (ncMatcher.find()) {
            String outerGetter = "get" + ManifestSpec.toClassName(ncMatcher.group(1));
            String innerKey = ncMatcher.group(2);
            body.addStatement("$T outerVal = config.$L()", String.class, outerGetter);
            body.addStatement("$T outerMap = ($T<?, ?>) MAPPER.readValue(outerVal, $T.class)",
                ClassName.get("java.util", "Map"), Map.class, Object.class);
            body.addStatement("$T privateKeyPem = ($T) outerMap.get($S)", String.class, String.class, innerKey);
        } else {
            body.addStatement("$T privateKeyPem = $L", String.class, interpolateTemplate(secretKey));
        }

        body.addStatement(
            "$T keyContent = privateKeyPem\n"
                + "        .replace(\"-----BEGIN PRIVATE KEY-----\", \"\")\n"
                + "        .replace(\"-----END PRIVATE KEY-----\", \"\")\n"
                + "        .replace(\"-----BEGIN RSA PRIVATE KEY-----\", \"\")\n"
                + "        .replace(\"-----END RSA PRIVATE KEY-----\", \"\")\n"
                + "        .replaceAll(\"\\\\s+\", \"\")",
            String.class);
        body.addStatement("byte[] keyBytes = $T.getDecoder().decode(keyContent)",
            ClassName.get("java.util", "Base64"));

        body.addStatement("long now = $T.currentTimeMillis() / 1000L", System.class);
        body.addStatement("long exp = now + $L", auth.getTokenDuration());
        body.addStatement("$T headerJson = \"{\\\"alg\\\":\\\"RS256\\\",\\\"typ\\\":\\\"JWT\\\"}\"",
            String.class);

        Map<String, String> payloadFields = new LinkedHashMap<>();
        if (auth.getJwtPayload() != null) payloadFields.putAll(auth.getJwtPayload());
        if (auth.getAdditionalJwtPayload() != null) payloadFields.putAll(auth.getAdditionalJwtPayload());

        body.addStatement("$T<$T, $T> payloadMap = new $T<>()",
            Map.class, String.class, Object.class, ClassName.get("java.util", "LinkedHashMap"));
        body.addStatement("payloadMap.put(\"iat\", now)");
        body.addStatement("payloadMap.put(\"exp\", exp)");
        for (Map.Entry<String, String> e : payloadFields.entrySet()) {
            Matcher jlPayload = jsonLoads.matcher(e.getValue());
            Matcher ncPayload = nestedConfig.matcher(e.getValue());
            if (jlPayload.find()) {
                String cfgGetter = "get" + ManifestSpec.toClassName(jlPayload.group(1));
                String jsonKey = jlPayload.group(2);
                body.addStatement(
                    "payloadMap.put($S, ($T)(($T<?,?>) MAPPER.readValue(config.$L(), $T.class)).get($S))",
                    e.getKey(), String.class, Map.class, cfgGetter, Object.class, jsonKey);
            } else if (ncPayload.find()) {
                String outerGetter = "get" + ManifestSpec.toClassName(ncPayload.group(1));
                String innerKey = ncPayload.group(2);
                body.addStatement(
                    "payloadMap.put($S, ($T)(($T<?,?>) MAPPER.readValue(config.$L(), $T.class)).get($S))",
                    e.getKey(), String.class, Map.class, outerGetter, Object.class, innerKey);
            } else {
                body.addStatement("payloadMap.put($S, $L)", e.getKey(), interpolateTemplate(e.getValue()));
            }
        }
        body.addStatement("$T payloadJson = MAPPER.writeValueAsString(payloadMap)", String.class);

        ClassName b64 = ClassName.get("java.util", "Base64");
        body.addStatement(
            "$T headerB64 = $T.getUrlEncoder().withoutPadding().encodeToString(headerJson.getBytes($T.UTF_8))",
            String.class, b64, ClassName.get("java.nio.charset", "StandardCharsets"));
        body.addStatement(
            "$T payloadB64 = $T.getUrlEncoder().withoutPadding().encodeToString(payloadJson.getBytes($T.UTF_8))",
            String.class, b64, ClassName.get("java.nio.charset", "StandardCharsets"));
        body.addStatement(
            "byte[] signingInput = (headerB64 + \".\" + payloadB64).getBytes($T.UTF_8)",
            ClassName.get("java.nio.charset", "StandardCharsets"));

        body.addStatement(
            "$T privateKey = $T.getInstance(\"RSA\").generatePrivate(new $T(keyBytes))",
            ClassName.get("java.security", "PrivateKey"),
            ClassName.get("java.security", "KeyFactory"),
            ClassName.get("java.security.spec", "PKCS8EncodedKeySpec"));
        body.addStatement(
            "$T sig = $T.getInstance(\"SHA256withRSA\")",
            ClassName.get("java.security", "Signature"),
            ClassName.get("java.security", "Signature"));
        body.addStatement("sig.initSign(privateKey)");
        body.addStatement("sig.update(signingInput)");
        body.addStatement(
            "$T sigB64 = $T.getUrlEncoder().withoutPadding().encodeToString(sig.sign())",
            String.class, b64);
        body.addStatement("return headerB64 + \".\" + payloadB64 + \".\" + sigB64");
        body.nextControlFlow("catch ($T e)", Exception.class);
        body.addStatement("throw new $T(\"Failed to build JWT\", e)", CONNECT_EXCEPTION);
        body.endControlFlow();

        return MethodSpec.methodBuilder("buildJwt")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class)
            .addCode(body.build())
            .build();
    }

    /**
     * Emit a poll method that delegates to {@code CustomComponentRegistry} for streams
     * whose retriever or requester carries a {@code class_name}. The registered Java impl
     * owns the network call, decoding, transformations, and state. We just drain its
     * iterator and wrap each record as a {@code SourceRecord}.
     *
     * <p>If no factory is registered for the class_name, registry lookup raises
     * {@code ConnectException} at first poll — connector loads, task fails fast.</p>
     */
    private MethodSpec buildCustomComponentPollMethod(
        StreamSpec stream,
        ParameterizedTypeName listOfSourceRecord
    ) {
        String streamName = stream.getName();
        String methodName = "poll" + ManifestSpec.toClassName(streamName);
        boolean retrieverDispatch = stream.getRetriever().getClassName() != null;
        String customClassName = retrieverDispatch
            ? stream.getRetriever().getClassName()
            : stream.getRetriever().getRequester().getClassName();
        ClassName customInterface = retrieverDispatch ? CUSTOM_RETRIEVER : CUSTOM_REQUESTER;

        ParameterizedTypeName mapStrObj = ParameterizedTypeName.get(
            ClassName.get("java.util", "Map"),
            ClassName.get(String.class), ClassName.get(Object.class));
        ParameterizedTypeName iterMapStrObj = ParameterizedTypeName.get(
            ClassName.get("java.util", "Iterator"), mapStrObj);
        ParameterizedTypeName iterJsonNode = ParameterizedTypeName.get(
            ClassName.get("java.util", "Iterator"), JSON_NODE);

        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("final $T streamName = $S", String.class, streamName);
        body.addStatement("$T<$T> result = new $T<>()", List.class, SOURCE_RECORD, ArrayList.class);
        body.addStatement("$T<$T, $T> _emptyParams = $T.emptyMap()",
            Map.class, String.class, Object.class, Collections.class);
        body.addStatement("$T _component = $T.create($S, $T.class, this.config.originalsStrings(), _emptyParams)",
            customInterface, CUSTOM_REGISTRY, customClassName, customInterface);
        body.beginControlFlow("try");
        if (retrieverDispatch) {
            body.addStatement("$T _records = _component.read(_emptyParams, _emptyParams)", iterMapStrObj);
            body.beginControlFlow("while (_records.hasNext())");
            body.addStatement("$T _record = _records.next()", mapStrObj);
            body.addStatement("$T _ctx = jinjaCtx(_record)", mapStrObj);
            body.addStatement("$T<$T> _kept = $L.process(_record, _ctx)",
                ClassName.get("java.util", "Optional"), mapStrObj, pipelineFieldName(streamName));
            body.beginControlFlow("if (_kept.isEmpty())");
            body.addStatement("continue");
            body.endControlFlow();
            body.addStatement("$T _value = MAPPER.writeValueAsString(_kept.get())", String.class);
            body.add(emitCustomSourceRecordAdd());
            body.endControlFlow();
        } else {
            body.addStatement("$T _nodes = _component.send(_emptyParams, _emptyParams)", iterJsonNode);
            body.beginControlFlow("while (_nodes.hasNext())");
            body.addStatement("$T _value = MAPPER.writeValueAsString(_nodes.next())", String.class);
            body.add(emitCustomSourceRecordAdd());
            body.endControlFlow();
        }
        body.nextControlFlow("catch ($T e)", Exception.class);
        body.addStatement(
            "throw new $T(\"Failed to poll \" + streamName + \" via custom component '\" + $S + \"'\", e)",
            CONNECT_EXCEPTION, customClassName);
        body.endControlFlow();
        body.addStatement("return result");

        return MethodSpec.methodBuilder(methodName)
            .addModifiers(Modifier.PRIVATE)
            .returns(listOfSourceRecord)
            .addException(InterruptedException.class)
            .addCode(body.build())
            .build();
    }

    private CodeBlock emitCustomSourceRecordAdd() {
        return CodeBlock.builder()
            .addStatement(
                "result.add(new $T($T.of($S, streamName), $T.of($S, $S), streamName, "
                    + "$T.STRING_SCHEMA, _value))",
                SOURCE_RECORD, Map.class, "stream", Map.class, "ts", "0", SCHEMA)
            .build();
    }

    /**
     * Generates a private {@code sendWithRetry()} method that delegates response classification
     * to {@link org.apache.kafka.connect.manifest.codegen.runtime.retry.RetryPolicy} and
     * sleep timing to {@link org.apache.kafka.connect.manifest.codegen.runtime.retry.backoff.BackoffStrategy}.
     *
     * <p>Mirrors Airbyte's HttpRequester._send loop (http_requester.py:550-606): classify, then
     * either return, fail, ignore, or sleep-and-retry. Exhausting {@code maxRetries} or
     * {@code maxTimeMillis} throws {@link org.apache.kafka.connect.errors.ConnectException}.</p>
     */
    private MethodSpec buildSendWithRetry() {
        ParameterizedTypeName httpResponseString = ParameterizedTypeName.get(HTTP_RESPONSE, ClassName.get(String.class));
        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("int attempt = 0");
        body.addStatement("long deadline = $T.currentTimeMillis() + retryPolicy.maxTimeMillis()",
            System.class);
        body.beginControlFlow("while (true)");
        body.addStatement(
            "$T<$T> resp = httpClient.send(request, $T.BodyHandlers.ofString())",
            HTTP_RESPONSE, String.class, HTTP_RESPONSE
        );
        body.addStatement("$T resolution = retryPolicy.interpretResponse(resp)", ERROR_RESOLUTION);
        body.addStatement("$T action = resolution.action()", RESPONSE_ACTION);
        body.beginControlFlow("if (action == $T.SUCCESS)", RESPONSE_ACTION);
        body.addStatement("return resp");
        body.endControlFlow();
        body.beginControlFlow("if (action == $T.IGNORE)", RESPONSE_ACTION);
        body.addStatement("return $T.empty(resp)", IGNORED_RESPONSES);
        body.endControlFlow();
        body.beginControlFlow("if (action == $T.FAIL)", RESPONSE_ACTION);
        body.addStatement(
            "throw new $T(resolution.errorMessage() != null ? resolution.errorMessage()"
                + " : \"Request failed with status \" + resp.statusCode())",
            CONNECT_EXCEPTION);
        body.endControlFlow();
        body.beginControlFlow("if (attempt >= retryPolicy.maxRetries() || $T.currentTimeMillis() >= deadline)",
            System.class);
        body.addStatement(
            "throw new $T(\"Exhausted retries (\" + (attempt + 1) + \" attempts) for status \" "
                + "+ resp.statusCode())",
            CONNECT_EXCEPTION);
        body.endControlFlow();
        body.addStatement("$T sleepMs = backoffStrategy.backoffMillis(resp, attempt)", Long.class);
        body.beginControlFlow("if (sleepMs != null && sleepMs > 0)");
        body.addStatement("$T.sleep(sleepMs)", Thread.class);
        body.endControlFlow();
        body.addStatement("attempt++");
        body.endControlFlow();

        return MethodSpec.methodBuilder("sendWithRetry")
            .addModifiers(Modifier.PRIVATE)
            .returns(httpResponseString)
            .addParameter(HTTP_REQUEST, "request")
            .addParameter(RETRY_POLICY, "retryPolicy")
            .addParameter(BACKOFF_STRATEGY, "backoffStrategy")
            .addException(Exception.class)
            .addCode(body.build())
            .build();
    }

    /**
     * Emits {@code HttpResponse<String> <respVar> = sendWithRetry(<reqVar>, <retryPolicy>,
     * <backoffStrategy>);} using the per-stream/per-requester {@code error_handler}.
     *
     * <p>This mirrors Airbyte Python's per-requester error handling: each declarative stream's
     * {@code HttpRequester} carries its own {@code error_handler} with its own
     * {@code response_filters} and {@code backoff_strategies}
     * (airbyte_cdk/sources/declarative/requesters/http_requester.py:37-110). A single shared
     * policy on the task would silently apply the first stream's filters to all streams.</p>
     */
    private void emitSendWithRetryCall(
        CodeBlock.Builder body,
        String respVar,
        String reqVar,
        RequesterSpec.ErrorHandlerSpec eh
    ) {
        body.addStatement("$T<$T> $L = sendWithRetry($L, $L, $L)",
            HTTP_RESPONSE, String.class, respVar, reqVar,
            retryPolicyExpr(eh),
            backoffChainExpr(eh));
    }

    /**
     * Builds a CodeBlock evaluating to a {@code RetryPolicy} expression for the given
     * error_handler spec. Falls back to {@code DefaultRetryPolicy.fallbackOnly()} when the
     * manifest has no error_handler. CompositeErrorHandler wraps child policies in a
     * {@link CompositeRetryPolicy}; everything else becomes a {@link DefaultRetryPolicy}.
     */
    private CodeBlock retryPolicyExpr(RequesterSpec.ErrorHandlerSpec eh) {
        if (eh == null) {
            return CodeBlock.of("$T.fallbackOnly()", DEFAULT_RETRY_POLICY);
        }
        if ("CompositeErrorHandler".equals(eh.getType())
            && eh.getErrorHandlers() != null
            && !eh.getErrorHandlers().isEmpty()) {
            CodeBlock.Builder list = CodeBlock.builder().add("$T.of(", List.class);
            boolean first = true;
            for (RequesterSpec.ErrorHandlerSpec child : eh.getErrorHandlers()) {
                if (!first) list.add(", ");
                list.add(retryPolicyExpr(child));
                first = false;
            }
            list.add(")");
            return CodeBlock.of("new $T($L)", COMPOSITE_RETRY_POLICY, list.build());
        }
        return CodeBlock.of("new $T($L, $L, $L)",
            DEFAULT_RETRY_POLICY,
            responseFiltersExpr(eh.getResponseFilters()),
            boxedIntOrNull(eh.getMaxRetries()),
            boxedIntOrNull(eh.getMaxTime()));
    }

    /**
     * Builds the {@code List<HttpResponseFilter>} expression used by DefaultRetryPolicy. Each
     * filter forwards through {@link HttpResponseFilter#from} so {@code action == null}
     * entries collapse to {@code null} and are skipped by the policy.
     */
    private CodeBlock responseFiltersExpr(List<RequesterSpec.ResponseFilterSpec> filters) {
        if (filters == null || filters.isEmpty()) {
            return CodeBlock.of("$T.emptyList()", Collections.class);
        }
        CodeBlock.Builder b = CodeBlock.builder().add("$T.of(", List.class);
        boolean first = true;
        for (RequesterSpec.ResponseFilterSpec f : filters) {
            if (!first) b.add(", ");
            b.add(singleResponseFilterExpr(f));
            first = false;
        }
        b.add(")");
        return b.build();
    }

    private CodeBlock singleResponseFilterExpr(RequesterSpec.ResponseFilterSpec f) {
        CodeBlock action = f.getAction() == null
            ? CodeBlock.of("null")
            : CodeBlock.of("$T.$L", RESPONSE_ACTION, f.getAction().toUpperCase(java.util.Locale.ROOT));
        CodeBlock httpCodes = (f.getHttpCodes() == null || f.getHttpCodes().isEmpty())
            ? CodeBlock.of("$T.emptyList()", Collections.class)
            : codesListExpr(f.getHttpCodes());
        return CodeBlock.of(
            "$T.from($L, $L, $L, $L, $L, $L, jinjaCtx())",
            HTTP_RESPONSE_FILTER,
            action,
            httpCodes,
            stringLiteralOrNull(f.getPredicate()),
            stringLiteralOrNull(f.getErrorMessageContains()),
            stringLiteralOrNull(f.getErrorMessage()),
            stringLiteralOrNull(f.getFailureType()));
    }

    private CodeBlock codesListExpr(List<Integer> codes) {
        CodeBlock.Builder b = CodeBlock.builder().add("$T.of(", List.class);
        boolean first = true;
        for (Integer c : codes) {
            if (!first) b.add(", ");
            b.add("$L", c);
            first = false;
        }
        b.add(")");
        return b.build();
    }

    /**
     * Builds the {@code BackoffStrategy} expression. An empty list yields a chain that
     * defaults to ExponentialBackoffStrategy (matching Airbyte's fallback in
     * DefaultErrorHandler.backoff_time when no strategy applies).
     */
    private CodeBlock backoffChainExpr(RequesterSpec.ErrorHandlerSpec eh) {
        List<RequesterSpec.BackoffStrategySpec> bs = eh == null ? null : eh.getBackoffStrategies();
        if (bs == null || bs.isEmpty()) {
            return CodeBlock.of("new $T($T.emptyList())", BACKOFF_STRATEGY_CHAIN, Collections.class);
        }
        CodeBlock.Builder list = CodeBlock.builder().add("$T.of(", List.class);
        boolean first = true;
        for (RequesterSpec.BackoffStrategySpec strat : bs) {
            if (!first) list.add(", ");
            list.add(singleBackoffExpr(strat));
            first = false;
        }
        list.add(")");
        return CodeBlock.of("new $T($L)", BACKOFF_STRATEGY_CHAIN, list.build());
    }

    private CodeBlock singleBackoffExpr(RequesterSpec.BackoffStrategySpec s) {
        String type = s.getType() == null ? "" : s.getType();
        switch (type) {
            case "ConstantBackoffStrategy":
                return CodeBlock.of("new $T($L)",
                    CONSTANT_BACKOFF, doubleOrZero(s.getBackoffTimeInSeconds()));
            case "WaitTimeFromHeader":
            case "WaitTimeFromHeaderBackoffStrategy":
                return CodeBlock.of("new $T($L, $L, $L)",
                    WAIT_TIME_FROM_HEADER,
                    stringLiteralOrNull(s.getHeader()),
                    stringLiteralOrNull(s.getRegex()),
                    boxedDoubleOrNull(s.getMaxWaitingTimeInSeconds()));
            case "WaitUntilTimeFromHeader":
            case "WaitUntilTimeFromHeaderBackoffStrategy":
                return CodeBlock.of("new $T($L, $L, $L)",
                    WAIT_UNTIL_TIME_FROM_HEADER,
                    stringLiteralOrNull(s.getHeader()),
                    stringLiteralOrNull(s.getRegex()),
                    boxedDoubleOrNull(s.getMinWait()));
            case "ExponentialBackoffStrategy":
            default:
                return CodeBlock.of("new $T($L)",
                    EXPONENTIAL_BACKOFF, boxedDoubleOrNull(s.getFactor()));
        }
    }

    private static CodeBlock stringLiteralOrNull(String s) {
        return s == null ? CodeBlock.of("null") : CodeBlock.of("$S", s);
    }

    private static CodeBlock boxedIntOrNull(Integer v) {
        return v == null ? CodeBlock.of("(Integer) null") : CodeBlock.of("$L", v);
    }

    private static CodeBlock boxedDoubleOrNull(Double v) {
        return v == null ? CodeBlock.of("(Double) null") : CodeBlock.of("$L", v + "d");
    }

    private static CodeBlock doubleOrZero(Double v) {
        return CodeBlock.of("$L", (v == null ? 0.0 : v) + "d");
    }

    private MethodSpec buildJinjaCtx() {
        ClassName mapClass = ClassName.get("java.util", "Map");
        ClassName linkedHashMap = ClassName.get("java.util", "LinkedHashMap");
        ParameterizedTypeName mapStringObject = ParameterizedTypeName.get(
            mapClass, ClassName.get(String.class), ClassName.get(Object.class));
        ClassName jsonConfigParser = ClassName.get(
            "org.apache.kafka.connect.manifest.codegen.runtime", "JsonConfigParser");
        return MethodSpec.methodBuilder("jinjaCtx")
            .addModifiers(Modifier.PRIVATE)
            .returns(mapStringObject)
            .addStatement("$T ctx = new $T<>()", mapStringObject, linkedHashMap)
            .beginControlFlow("if (configValues != null)")
            .addStatement("ctx.put($S, $T.parseObjectValues(configValues))", "config", jsonConfigParser)
            .nextControlFlow("else if (config != null)")
            .addStatement("@$T($S) $T<$T, $T> _rawStrings = ($T<$T, $T>) ($T<?, ?>) config.originalsStrings()",
                ClassName.get("java.lang", "SuppressWarnings"), "unchecked",
                mapClass, ClassName.get(String.class), ClassName.get(Object.class),
                mapClass, ClassName.get(String.class), ClassName.get(Object.class),
                mapClass)
            .addStatement("ctx.put($S, $T.parseObjectValues(_rawStrings))", "config", jsonConfigParser)
            .endControlFlow()
            .addStatement("return ctx")
            .build();
    }

    private MethodSpec buildJinjaCtxWithRecord() {
        ClassName mapClass = ClassName.get("java.util", "Map");
        ParameterizedTypeName mapStringObject = ParameterizedTypeName.get(
            mapClass, ClassName.get(String.class), ClassName.get(Object.class));
        return MethodSpec.methodBuilder("jinjaCtx")
            .addModifiers(Modifier.PRIVATE)
            .returns(mapStringObject)
            .addParameter(mapStringObject, "record")
            .addStatement("$T ctx = jinjaCtx()", mapStringObject)
            .beginControlFlow("if (record != null)")
            .addStatement("ctx.put($S, record)", "record")
            .endControlFlow()
            .addStatement("return ctx")
            .build();
    }

    private MethodSpec buildJinjaCtxForStream(List<StreamSpec> streams) {
        ClassName mapClass = ClassName.get("java.util", "Map");
        ClassName linkedHashMap = ClassName.get("java.util", "LinkedHashMap");
        ParameterizedTypeName mapStringObject = ParameterizedTypeName.get(
            mapClass, ClassName.get(String.class), ClassName.get(Object.class));
        MethodSpec.Builder b = MethodSpec.methodBuilder("jinjaCtxForStream")
            .addModifiers(Modifier.PRIVATE)
            .returns(mapStringObject)
            .addParameter(String.class, "streamName")
            .addStatement("$T ctx = jinjaCtx()", mapStringObject)
            .addStatement("$T<$T, $T> params = new $T<>()",
                mapClass, ClassName.get(String.class), ClassName.get(Object.class), linkedHashMap)
            .addStatement("params.put($S, streamName)", "name");
        boolean anyParams = streams.stream().anyMatch(s -> !s.getParameters().isEmpty());
        if (anyParams) {
            b.beginControlFlow("switch (streamName)");
            for (StreamSpec stream : streams) {
                Map<String, Object> p = stream.getParameters();
                if (p.isEmpty() || stream.getName() == null) continue;
                b.addCode("case $S:\n", stream.getName());
                b.addCode("$>");
                for (Map.Entry<String, Object> e : p.entrySet()) {
                    if ("name".equals(e.getKey())) continue;
                    emitParamPut(b, e.getKey(), e.getValue());
                }
                b.addStatement("break");
                b.addCode("$<");
            }
            b.addCode("default: break;\n");
            b.endControlFlow();
        }
        return b.addStatement("ctx.put($S, params)", "parameters")
                .addStatement("return ctx")
                .build();
    }

    /**
     * Emits {@code params.put("key", literal)} where the value literal is rendered from a
     * manifest {@code $parameters} entry. Strings, numbers, and booleans become Java literals
     * directly; lists of strings become {@code List.of(...)}. Other shapes (nested maps,
     * mixed lists) are skipped with a codegen log line — they are extremely rare in practice
     * and can be extended without changing the generated-task contract.
     */
    private void emitParamPut(MethodSpec.Builder b, String key, Object value) {
        if (value == null) {
            b.addStatement("params.put($S, null)", key);
            return;
        }
        if (value instanceof String) {
            b.addStatement("params.put($S, $S)", key, value);
            return;
        }
        if (value instanceof Boolean || value instanceof Integer || value instanceof Long) {
            b.addStatement("params.put($S, $L)", key, value);
            return;
        }
        if (value instanceof Double || value instanceof Float) {
            b.addStatement("params.put($S, $LD)", key, value);
            return;
        }
        if (value instanceof List<?> list && list.stream().allMatch(v -> v instanceof String)) {
            StringBuilder fmt = new StringBuilder("params.put($S, $T.of(");
            Object[] args = new Object[list.size() + 2];
            args[0] = key;
            args[1] = ClassName.get("java.util", "List");
            for (int i = 0; i < list.size(); i++) {
                fmt.append(i == 0 ? "$S" : ", $S");
                args[i + 2] = list.get(i);
            }
            fmt.append("))");
            b.addStatement(fmt.toString(), args);
            return;
        }
        // Unsupported structural value — leave the key absent so Jinja resolves to undefined.
        // Real-world manifests use scalars or list-of-strings; revisit if a manifest needs more.
    }

    private MethodSpec buildJinjaCtxWithSlice() {
        ClassName mapClass = ClassName.get("java.util", "Map");
        ClassName linkedHashMap = ClassName.get("java.util", "LinkedHashMap");
        ParameterizedTypeName mapStringObject = ParameterizedTypeName.get(
            mapClass, ClassName.get(String.class), ClassName.get(Object.class));
        return MethodSpec.methodBuilder("jinjaCtxWithSlice")
            .addModifiers(Modifier.PRIVATE)
            .returns(mapStringObject)
            .addParameter(String.class, "streamName")
            .addParameter(String.class, "startTime")
            .addParameter(String.class, "endTime")
            .addStatement("$T ctx = jinjaCtxForStream(streamName)", mapStringObject)
            .addStatement("$T<$T, $T> slice = new $T<>()",
                mapClass, ClassName.get(String.class), ClassName.get(Object.class), linkedHashMap)
            .addStatement("slice.put($S, startTime)", "start_time")
            .addStatement("slice.put($S, endTime)", "end_time")
            .addStatement("ctx.put($S, slice)", "stream_slice")
            .addStatement("ctx.put($S, slice)", "stream_interval")
            .addStatement("return ctx")
            .build();
    }

    private MethodSpec buildStop() {
        return MethodSpec.methodBuilder("stop")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .build();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Emits a Java expression that yields the rendered value of {@code template} at runtime.
     * Plain literals (no {@code {{ } or {% %} ) collapse to a Java string literal; everything
     * else becomes {@code render("template", jinjaCtx())}, deferring Jinja semantics to
     * {@link org.apache.kafka.connect.manifest.codegen.runtime.jinja.JinjaRenderer}.
     */
    private String interpolateTemplate(String template) {
        return JinjaSnippets.interpolateTemplate(template, currentSpecPropKeys);
    }

    /**
     * Like {@link #interpolateTemplate} but generates {@code render("...", jinjaCtxForStream(streamName))}
     * so that Jinja templates referencing {@code parameters['name']} resolve to the current stream's name.
     * Used for request_parameters where Airbyte CDK injects {@code parameters.name = stream.name}.
     */
    private String interpolateTemplateWithStream(String template) {
        if (template == null || template.isEmpty()) return "\"\"";
        if (!template.contains("{{") && !template.contains("{%")) {
            return "\"" + JinjaSnippets.escapeJavaString(template) + "\"";
        }
        return "render(\"" + JinjaSnippets.escapeJavaString(template) + "\", jinjaCtxForStream(streamName))";
    }

    /**
     * Like {@link #interpolateTemplateWithStream} but also injects {@code stream_slice} with
     * {@code start_time}/{@code end_time} for DatetimeBasedCursor window-sliced streams.
     * Airbyte CDK puts window bounds in {@code stream_slice} so request_parameters can reference
     * {@code stream_slice['start_time']} and {@code stream_slice['end_time']}.
     *
     * @param startVar  Java variable name holding the window start (cursor variable)
     * @param endVar    Java variable name holding the window end (typically {@code _windowEnd})
     */
    private String interpolateTemplateWithStreamSlice(String template, String startVar, String endVar) {
        if (template == null || template.isEmpty()) return "\"\"";
        if (!template.contains("{{") && !template.contains("{%")) {
            return "\"" + JinjaSnippets.escapeJavaString(template) + "\"";
        }
        return "render(\"" + JinjaSnippets.escapeJavaString(template) + "\", jinjaCtxWithSlice(streamName, "
            + startVar + ", " + endVar + "))";
    }

    /** Returns the header name for an ApiKeyAuthenticator (inject_into.field_name or legacy header field). */
    private String resolveApiKeyHeaderName(AuthenticatorSpec auth) {
        if (auth.getInjectInto() != null && auth.getInjectInto().getFieldName() != null) {
            return auth.getInjectInto().getFieldName();
        }
        if (auth.getHeader() != null) {
            return auth.getHeader();
        }
        return "X-API-Key";
    }

    private List<String> extractFieldPath(StreamSpec stream) {
        if (stream.getRetriever() == null) {
            return Collections.emptyList();
        }
        RecordSelectorSpec selector = stream.getRetriever().getRecordSelector();
        if (selector == null || selector.getExtractor() == null) {
            return Collections.emptyList();
        }
        List<String> fp = selector.getExtractor().getFieldPath();
        if (fp == null) {
            return Collections.emptyList();
        }
        List<String> expanded = new ArrayList<>(fp.size());
        for (String seg : fp) {
            expanded.add(expandStreamParams(seg, stream));
        }
        return expanded;
    }

    /**
     * Substitutes {@code {{ parameters['x'] }}}, {@code {{ parameters["x"] }}},
     * {@code {{ parameters.get('x') }}}, {@code {{ parameters.get("x") }}}, and
     * {@code {{ parameters.x }}} references with the stream's {@code $parameters}
     * value at codegen time.
     *
     * <p>Needed when these references appear in identifiers that the codegen embeds as
     * literal Java strings (map keys, URL fragments, field names) rather than as
     * Jinja templates rendered at runtime. Airbyte's runtime renders both with the
     * same templating engine, but we can only render runtime-rendered strings via
     * {@code render(...)}; literal-key sites need the value baked in.</p>
     *
     * <p>If a referenced parameter is missing or the value is non-scalar, the original
     * substring is left in place — codegen consumers that depended on the literal
     * Jinja fragment (none currently) continue to work.</p>
     */
    private static String expandStreamParams(String template, StreamSpec stream) {
        if (template == null || template.isEmpty() || !template.contains("{{")) {
            return template;
        }
        Map<String, Object> params = stream.getParameters();
        if (params == null || params.isEmpty()) {
            return template;
        }
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
            "\\{\\{\\s*parameters(?:\\[['\"]([^'\"]+)['\"]\\]"
                + "|\\.get\\(['\"]([^'\"]+)['\"]\\)"
                + "|\\.([A-Za-z_][A-Za-z0-9_]*))\\s*\\}\\}");
        java.util.regex.Matcher m = p.matcher(template);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String key = m.group(1) != null ? m.group(1)
                : m.group(2) != null ? m.group(2) : m.group(3);
            Object val = params.get(key);
            String replacement = (val instanceof String || val instanceof Number || val instanceof Boolean)
                ? String.valueOf(val)
                : m.group();
            m.appendReplacement(out, java.util.regex.Matcher.quoteReplacement(replacement));
        }
        m.appendTail(out);
        return out.toString();
    }

    private static String toJavaName(String streamName) {
        // Strip file-like extensions (e.g. ".json" suffix in some Airbyte stream names)
        String n = streamName.replaceAll("\\.[a-z]+$", "");
        // Replace all non-alphanumeric chars with underscore
        n = n.replaceAll("[^a-zA-Z0-9]", "_");
        // Collapse consecutive underscores and strip leading/trailing underscores
        n = n.replaceAll("_+", "_").replaceAll("^_|_$", "");
        // Prefix with _ if starts with a digit
        if (!n.isEmpty() && Character.isDigit(n.charAt(0))) n = "_" + n;
        return n.isEmpty() ? "stream" : n;
    }

    private static String cursorFieldName(String streamName) {
        return "cursor_" + toJavaName(streamName);
    }

    private static String partitionKeysFieldName(String streamName) {
        return toJavaName(streamName) + "_partitionKeys";
    }

    private static String partitionIdxFieldName(String streamName) {
        return toJavaName(streamName) + "_partitionIdx";
    }

    private static String pipelineFieldName(String streamName) {
        return "pipeline_" + toJavaName(streamName);
    }

    private static String configTransformsJson(ManifestSpec spec) {
        List<ConfigTransformationSpec> specs = spec.getConfigTransformations();
        return ConfigTransformerFactory.toJson(specs);
    }

    private static String filterConditionFor(StreamSpec stream) {
        RecordSelectorSpec selector = stream.getRetriever() != null
            ? stream.getRetriever().getRecordSelector() : null;
        if (selector == null || selector.getRecordFilter() == null) {
            return null;
        }
        return selector.getRecordFilter().getCondition();
    }

    /**
     * Generates a Java variable name for a ListPartitionRouter loop variable.
     * e.g. cursorField="breakdown" → "_lp_breakdown"; null → "_lp_partition".
     */
    private static String listLoopVar(String cursorField) {
        return "_lp_" + (cursorField != null ? cursorField : "partition").replaceAll("[^a-zA-Z0-9]", "_");
    }

    /** Returns the loop variable for the router whose cursorField matches {@code field}, or null. */
    private static String findLoopVarForField(List<PartitionRouterSpec> routers, String field) {
        if (field == null || routers == null) return null;
        for (PartitionRouterSpec lr : routers) {
            if (field.equals(lr.getCursorField())) {
                return listLoopVar(lr.getCursorField());
            }
        }
        return null;
    }

    /**
     * Generates a poll method that wraps the normal fetch in nested for-loops, one per
     * ListPartitionRouter, emitting one set of records per Cartesian combination of values.
     * Python CDK: list_partition_router.py + SimpleRetriever.stream_slices (itertools.product).
     */
    private MethodSpec buildListRouterPollMethod(
        StreamSpec stream,
        ClassName configClass,
        AuthenticatorSpec auth,
        ParameterizedTypeName listOfSourceRecord,
        List<PartitionRouterSpec> listRouters
    ) throws CodegenException {
        RequesterSpec requester = stream.getRetriever().getRequester();
        String baseUrl = requester.effectiveBaseUrl();
        String rawPath = requester.getPath();
        if (!baseUrl.isEmpty() && !baseUrl.endsWith("/") && !rawPath.isEmpty() && !rawPath.startsWith("/")) {
            baseUrl = baseUrl + "/";
        }
        String streamName = stream.getName();
        String methodName = "poll" + ManifestSpec.toClassName(streamName);
        PaginatorSpec paginator = stream.getRetriever().getPaginator();
        boolean hasPagination = paginator != null && !paginator.hasNoPagination();
        List<String> fieldPath = extractFieldPath(stream);

        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("final $T streamName = $S", String.class, streamName);
        body.addStatement("$T<$T> result = new $T<>()", List.class, SOURCE_RECORD, ArrayList.class);

        // Open one for-loop per ListPartitionRouter (Cartesian product via nesting).
        for (PartitionRouterSpec lr : listRouters) {
            String lv = listLoopVar(lr.getCursorField());
            List<String> values = lr.getValues();
            if (values.size() == 1 && values.get(0).startsWith("{{")) {
                // Jinja config-ref: resolve to a config getter call at codegen time.
                String expr = interpolateTemplate(values.get(0));
                // The resolved expr is a comma-separated string; split at runtime.
                body.beginControlFlow(
                    "for ($T $L : $L.split($S))", String.class, lv, expr, ",");
                body.addStatement("$L = $L.strip()", lv, lv);
            } else {
                // Literal list: emit List.of("a","b",...).
                StringBuilder fmt = new StringBuilder("for ($T $L : $T.of(");
                List<Object> args = new ArrayList<>();
                args.add(String.class);
                args.add(lv);
                args.add(List.class);
                for (int i = 0; i < values.size(); i++) {
                    if (i > 0) fmt.append(", ");
                    fmt.append("$S");
                    args.add(values.get(i));
                }
                fmt.append("))");
                body.beginControlFlow(fmt.toString(), args.toArray());
            }
        }

        // Declare pagination state variables fresh per partition-value combination.
        // Mirrors buildSubstreamPollMethod lines 571-588: per-slice, not per-poll.
        if (hasPagination && paginator != null) {
            if (paginator.isCursor()) {
                body.addStatement("$T nextCursor = null", String.class);
                // RequestPath paginators use the cursor as the full next URL; declare url too.
                if (isRequestPath(paginator)) {
                    body.addStatement("$T url = null", String.class);
                }
            } else if (paginator.isPageIncrement()) {
                int start = paginator.getPaginationStrategy() != null
                    ? paginator.getPaginationStrategy().getStartFromPage() : 1;
                body.addStatement("final int startPage = $L", start);
                body.addStatement("int page = startPage");
                body.addStatement("final int pageLimit = $L", paginator.pageSize());
            } else if (paginator.isOffsetIncrement()) {
                body.addStatement("int offset = 0");
                body.addStatement("final int pageLimit = $L", paginator.pageSize());
            }
        }

        body.add(buildListRouterUrlBlock(baseUrl, rawPath, listRouters, paginator, hasPagination, requester));

        body.beginControlFlow("try");
        buildRequestStatement(body, auth, paginator, requester);
        body.add(buildFetchBlock(paginator, stream));
        if (hasPagination && paginator.isCursor()) {
            body.add(buildCursorStateUpdate(paginator));
        }
        if (isCustomExtractor(stream)) {
            body.add(buildCustomExtractorNavBlock(stream.getRetriever().getRecordSelector().getExtractor().getClassName()));
        } else {
            body.add(buildFieldPathNav(fieldPath, hasPagination));
        }
        body.add(buildNormalizeAndCollect(hasPagination, paginator, null, null,
            pipelineFieldName(streamName)));
        body.nextControlFlow("catch ($T e)", InterruptedException.class);
        body.addStatement("$T.currentThread().interrupt()", Thread.class);
        body.addStatement("throw new $T(\"Interrupted while polling \" + streamName, e)", CONNECT_EXCEPTION);
        body.nextControlFlow("catch ($T e)", Exception.class);
        body.addStatement("throw new $T(\"Failed to poll \" + streamName, e)", CONNECT_EXCEPTION);
        body.endControlFlow();

        if (hasPagination) {
            body.add(buildPaginationLoopClose(paginator));
        }

        // Close for-loops in reverse (innermost first).
        for (int i = 0; i < listRouters.size(); i++) {
            body.endControlFlow();
        }

        body.addStatement("return result");

        return MethodSpec.methodBuilder(methodName)
            .addModifiers(Modifier.PRIVATE)
            .returns(listOfSourceRecord)
            .addException(InterruptedException.class)
            .addCode(body.build())
            .build();
    }

    /**
     * Returns a Pattern that matches both dot-notation (stream_partition.field)
     * and bracket-notation (stream_partition['field'] / stream_partition["field"]) Jinja2 refs.
     * Python CDK: declarative_component_schema.yaml — partition_field is accessed via either syntax.
     */
    private static Pattern streamPartitionPattern(String cursorField) {
        String q = Pattern.quote(cursorField);
        return Pattern.compile(
            "\\{\\{\\s*(?:stream_partition|stream_slice)(?:\\." + q
            + "|\\[\\s*['\"]" + q + "['\"]\\s*\\])\\s*\\}\\}");
    }

    /**
     * Builds the URL construction block for a stream with one or more ListPartitionRouters.
     * Substitutes stream_partition.X in the path with the matching loop variable,
     * appends query params from request_option.inject_into=request_parameter,
     * and appends requester request_parameters (e.g. api_token, limit).
     */
    private CodeBlock buildListRouterUrlBlock(
        String baseUrl, String rawPath,
        List<PartitionRouterSpec> listRouters,
        PaginatorSpec paginator, boolean hasPagination,
        RequesterSpec requester
    ) {
        CodeBlock.Builder b = CodeBlock.builder();

        // Check if the path contains any stream_partition.X or stream_partition['X'] references.
        // Python CDK supports both dot notation (stream_partition.field) and
        // bracket notation (stream_partition['field'], stream_partition["field"]).
        boolean hasPartitionTemplate = false;
        for (PartitionRouterSpec lr : listRouters) {
            if (lr.getCursorField() != null) {
                Matcher m = streamPartitionPattern(lr.getCursorField()).matcher(rawPath);
                if (m.find()) {
                    hasPartitionTemplate = true;
                    break;
                }
            }
        }

        if (!hasPartitionTemplate) {
            // Use addInitialUrlStatement so config-template base URLs like
            // url_base: "{{ config['server_url'] }}" resolve to config.getXxx() calls.
            addInitialUrlStatement(b, baseUrl, rawPath);
        } else {
            // Build list of (regex-pattern, loop-var) substitutions.
            List<String[]> subs = new ArrayList<>();
            for (PartitionRouterSpec lr : listRouters) {
                if (lr.getCursorField() != null) {
                    String placeholder = streamPartitionPattern(lr.getCursorField()).pattern();
                    subs.add(new String[]{placeholder, listLoopVar(lr.getCursorField())});
                }
            }

            // Split on the first pattern to get PREFIX and SUFFIX around the loop variable.
            String[] halves = rawPath.split(subs.get(0)[0], 2);
            String before = halves[0];
            String after = halves.length > 1 ? halves[1] : "";
            String firstLoopVar = subs.get(0)[1];

            // Substitute any remaining patterns in the suffix as literal string concat.
            for (int i = 1; i < subs.size(); i++) {
                after = after.replaceAll(subs.get(i)[0], "\" + " + subs.get(i)[1] + " + \"");
            }

            // Build the constructor expression, resolving any config-template in baseUrl.
            // e.g. url_base="{{ config['server_url'] }}" → config.getServerUrl() + prefix + encode(loopVar) + suffix
            StringBuilder fmt = new StringBuilder("$T urlBuilder = new $T(");
            List<Object> fmtArgs = new ArrayList<>();
            fmtArgs.add(StringBuilder.class);
            fmtArgs.add(StringBuilder.class);
            boolean needsPlus = appendUrlSegment(fmt, fmtArgs, joinUrl(baseUrl, before), false);
            if (needsPlus) fmt.append(" + ");
            fmt.append("$T.encode($L, $T.UTF_8)");
            fmtArgs.add(ClassName.get("java.net", "URLEncoder"));
            fmtArgs.add(firstLoopVar);
            fmtArgs.add(ClassName.get("java.nio.charset", "StandardCharsets"));
            if (!after.isEmpty()) {
                fmt.append(" + $S");
                fmtArgs.add(after);
            }
            fmt.append(")");
            b.addStatement(fmt.toString(), fmtArgs.toArray());
        }

        // Append request_option query params for each router.
        boolean firstParam = !rawPath.contains("?") && !hasPartitionTemplate;
        for (PartitionRouterSpec lr : listRouters) {
            PartitionRouterSpec.RequestOptionSpec opt = lr.getRequestOption();
            if (opt != null && opt.isRequestParameter() && opt.getFieldName() != null) {
                String lv = listLoopVar(lr.getCursorField());
                b.addStatement("urlBuilder.append($S).append($L)",
                    (firstParam ? "?" : "&") + opt.getFieldName() + "=", lv);
                firstParam = false;
            }
        }

        // Append requester request_parameters (e.g. api_token, limit).
        // Mirrors buildUrlBlock lines 1306-1325: Jinja templates are interpolated,
        // literal values are URL-encoded at codegen time.
        // Special case: {{ stream_partition.field }} / {{ stream_slice.field }} references
        // must resolve to the loop variable, not a runtime render() call.
        if (requester != null) {
            for (Map.Entry<String, String> entry : requester.getRequestParameters().entrySet()) {
                String tmpl = entry.getValue();
                if (tmpl == null) continue;
                String sep = firstParam ? "?" : "&";
                Matcher spMatch = STREAM_PARTITION_RE.matcher(tmpl.trim());
                if (spMatch.matches()) {
                    String field = spMatch.group(1) != null ? spMatch.group(1) : spMatch.group(2);
                    String lv = findLoopVarForField(listRouters, field);
                    if (lv != null) {
                        b.addStatement("urlBuilder.append($S).append($T.encode($L, $T.UTF_8))",
                            sep + entry.getKey() + "=",
                            ClassName.get("java.net", "URLEncoder"),
                            lv,
                            ClassName.get("java.nio.charset", "StandardCharsets"));
                        firstParam = false;
                        continue;
                    }
                }
                if (tmpl.contains("{{") || tmpl.contains("{%")) {
                    b.addStatement("urlBuilder.append($S + $T.encode($T.valueOf($L).trim(), $T.UTF_8))",
                        sep + entry.getKey() + "=",
                        ClassName.get("java.net", "URLEncoder"),
                        ClassName.get(String.class), interpolateTemplateWithStream(tmpl),
                        ClassName.get("java.nio.charset", "StandardCharsets"));
                } else {
                    String encoded = URLEncoder.encode(tmpl, StandardCharsets.UTF_8);
                    b.addStatement("urlBuilder.append($S)", sep + entry.getKey() + "=" + encoded);
                }
                firstParam = false;
            }
        }

        if (hasPagination && paginator != null) {
            appendPaginationParams(b, paginator, !rawPath.contains("?"),
                isBodyInjectedJson(paginator) || isBodyInjectedData(paginator));
        }

        // For RequestPath cursor paginators, the cursor IS the next URL. On page 1, use the
        // constructed URL; on page 2+, nextCursor replaces the entire URL.
        if (isRequestPath(paginator)) {
            b.addStatement("url = (nextCursor != null) ? nextCursor : urlBuilder.toString()");
        }

        return b.build();
    }

    // ── Multi-level (nested) SubstreamPartitionRouter support ───────────────────
    //
    // A child stream like google_classroom.studentsubmissions has two
    // SubstreamPartitionRouters whose parents form a nested chain:
    //   Router 1: parent=courses
    //   Router 2: parent=coursework  (coursework is itself a substream of courses)
    //   Path:     /v1/courses/{{stream_partition.course}}/courseWork/
    //             {{stream_partition.coursework}}/studentSubmissions
    //
    // We emit a {@code fetchXxxPartitionKeys()} method that walks the parent
    // chain via per-level fetch helpers and accumulates {@code Map<String,String>}
    // partition entries. The poll method consumes one entry per call, exposing the
    // partition map to the URL via JinjaRenderer's {@code stream_partition} key.
    //
    // Limitations of this first cut (acceptable per CLAUDE.md §1 spec-correctness
    // bar — record set is identical to Airbyte for typical small parent counts;
    // pagination support on parent fetches comes in a follow-up):
    //   - Parent-stream pagination is ignored: only the first page of each level
    //     is consumed.
    //   - Parent-fetch failures (non-2xx) skip that branch silently.

    /**
     * Returns the ordered chain of substream routers when {@code child} matches the
     * nested pattern (router N's parent is itself a substream of router N-1's parent),
     * or {@code null} otherwise.
     */
    private List<PartitionRouterSpec> multiSubstreamChain(StreamSpec child, ManifestSpec spec) {
        List<PartitionRouterSpec> subs = child.getRetriever().getPartitionRouter().stream()
            .filter(PartitionRouterSpec::isSubstream)
            .collect(java.util.stream.Collectors.toList());
        if (subs.size() < 2) return null;
        for (PartitionRouterSpec r : subs) {
            if (r.parentStreamName() == null || r.parentKey() == null || r.partitionField() == null) {
                return null;
            }
            StreamSpec ps = lookupStream(spec, r.parentStreamName());
            if (ps == null || ps.getRetriever() == null || ps.getRetriever().getRequester() == null) {
                return null;
            }
        }
        for (int i = 1; i < subs.size(); i++) {
            StreamSpec parentN = lookupStream(spec, subs.get(i).parentStreamName());
            PartitionRouterSpec parentNRouter = parentN.getRetriever().getSubstreamRouter();
            if (parentNRouter == null) return null;
            String prev = subs.get(i - 1).parentStreamName();
            if (prev == null || !prev.equals(parentNRouter.parentStreamName())) {
                return null;
            }
        }
        return subs;
    }

    private StreamSpec lookupStream(ManifestSpec spec, String name) {
        if (name == null) return null;
        return spec.resolvedStreams().stream()
            .filter(s -> name.equals(s.getName()))
            .findFirst().orElse(null);
    }

    /**
     * Emits {@code fetchXxxPartitionKeys()} plus one private level helper per router.
     * Each level helper fetches its parent stream's records and produces the partition
     * maps for the next level (or the final {@code Map<String,String>} list).
     */
    private MethodSpec buildNestedFetchPartitionKeys(
        StreamSpec childStream,
        List<PartitionRouterSpec> chain,
        AuthenticatorSpec auth,
        ManifestSpec spec
    ) throws CodegenException {
        String childName = childStream.getName();
        String orchestratorName = "fetch" + ManifestSpec.toClassName(childName) + "PartitionKeys";

        ParameterizedTypeName mapStringString = ParameterizedTypeName.get(
            ClassName.get("java.util", "Map"),
            ClassName.get(String.class), ClassName.get(String.class));
        ParameterizedTypeName listMap = ParameterizedTypeName.get(
            ClassName.get("java.util", "List"), mapStringString);

        // Emit a single method body that inlines per-level fetches using suffixed
        // variable names (_req1/_resp1, _req2/_resp2, ...) to avoid Java's
        // no-shadowing rule for nested local declarations.
        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("$T keys = new $T<>()", listMap, ArrayList.class);

        // Open one nested for-loop per level, inlining the HTTP fetch + extraction.
        for (int i = 0; i < chain.size(); i++) {
            PartitionRouterSpec r = chain.get(i);
            StreamSpec parentStream = lookupStream(spec, r.parentStreamName());
            String suffix = String.valueOf(i + 1);
            emitNestedFetchLevel(body, parentStream, r, auth, suffix, i == 0);
        }

        // Innermost: emit the partition Map for the deepest level.
        // _p<N> is built up from each level's parent_key extraction.
        // We emit a single LinkedHashMap that copies the previous level's _p and
        // adds this level's partition_field → extracted value.
        // Built progressively inside emitNestedFetchLevel via _p<level>.
        // At innermost level, accumulate into keys.
        body.addStatement("keys.add(_p$L)", chain.size());

        // Close all nested for-loops.
        for (int i = 0; i < chain.size(); i++) {
            body.endControlFlow();
        }

        body.addStatement("return keys");

        return MethodSpec.methodBuilder(orchestratorName)
            .addModifiers(Modifier.PRIVATE)
            .returns(listMap)
            .addException(Exception.class)
            .addCode(body.build())
            .build();
    }

    /**
     * Emits one level of the nested-fetch loop:
     *   - URL build (rendered through JinjaRenderer with cumulative stream_partition)
     *   - Auth + GET via {@code _reqN}/{@code _respN}
     *   - 2xx check (skip-on-failure: continue/return-keys)
     *   - field-path navigation
     *   - opens a {@code for (Object _recN : _recordsN)} loop and extracts {@code _kN}
     *   - copies previous {@code _p<N-1>} → {@code _p<N>} and adds the new partition_field
     */
    private void emitNestedFetchLevel(
        CodeBlock.Builder body,
        StreamSpec parentStream,
        PartitionRouterSpec router,
        AuthenticatorSpec auth,
        String suffix,
        boolean isFirstLevel
    ) throws CodegenException {
        RequesterSpec parentRequester = parentStream.getRetriever().getRequester();
        String baseUrl = parentRequester.effectiveBaseUrl();
        String path = parentRequester.getPath();
        if (!baseUrl.isEmpty() && !baseUrl.endsWith("/") && !path.isEmpty() && !path.startsWith("/")) {
            baseUrl = baseUrl + "/";
        }
        String fullUrlTemplate = joinUrl(baseUrl, path);
        List<String> fieldPath = extractFieldPath(parentStream);
        String partitionField = router.partitionField();
        String parentKey = router.parentKey();

        ParameterizedTypeName mapStringString = ParameterizedTypeName.get(
            ClassName.get("java.util", "Map"),
            ClassName.get(String.class), ClassName.get(String.class));
        ParameterizedTypeName mapStringObject = ParameterizedTypeName.get(
            ClassName.get("java.util", "Map"),
            ClassName.get(String.class), ClassName.get(Object.class));

        // Build URL: render via JinjaRenderer with cumulative stream_partition map.
        if (isFirstLevel) {
            // No accumulated context yet — but path may still reference config.
            if (!fullUrlTemplate.contains("{{") && !fullUrlTemplate.contains("{%")) {
                body.addStatement("$T _url$L = $S", String.class, suffix, fullUrlTemplate);
            } else {
                body.addStatement("$T _url$L = render($S, jinjaCtx())",
                    String.class, suffix, fullUrlTemplate);
            }
        } else {
            // Pass cumulative partition (_p<level-1>) through stream_partition.
            int prev = Integer.parseInt(suffix) - 1;
            body.addStatement("$T _ctx$L = jinjaCtx()", mapStringObject, suffix);
            body.addStatement("_ctx$L.put($S, _p$L)", suffix, "stream_partition", prev);
            body.addStatement("$T _url$L = render($S, _ctx$L)",
                String.class, suffix, fullUrlTemplate, suffix);
        }

        // Build + send request, with full auth.
        emitAuthedGet(body, auth, parentRequester, "_url" + suffix, "_req" + suffix, "_resp" + suffix);

        // Skip branch on non-2xx: continue if not first-level (we're inside a for-loop),
        // else return whatever we have so far.
        body.beginControlFlow("if (_resp$L.statusCode() < 200 || _resp$L.statusCode() >= 300)",
            suffix, suffix);
        body.addStatement(isFirstLevel ? "return keys" : "continue");
        body.endControlFlow();
        body.beginControlFlow("if (_resp$L.body() == null || _resp$L.body().isBlank())", suffix, suffix);
        body.addStatement(isFirstLevel ? "return keys" : "continue");
        body.endControlFlow();

        body.addStatement("$T _json$L = MAPPER.readValue(_resp$L.body(), $T.class)",
            Object.class, suffix, suffix, Object.class);

        // Navigate field path (each segment unwraps a Map).
        for (String seg : fieldPath) {
            body.beginControlFlow("if (_json$L instanceof $T<?,?> _fpm$L)",
                suffix, Map.class, suffix);
            body.addStatement("_json$L = _fpm$L.get($S)", suffix, suffix, seg);
            body.endControlFlow();
        }

        // Normalise to List<Object>.
        body.addStatement("$T<$T> _records$L", List.class, Object.class, suffix);
        body.beginControlFlow("if (_json$L instanceof $T)", suffix, List.class);
        body.addStatement("_records$L = ($T<$T>) _json$L", suffix, List.class, Object.class, suffix);
        body.nextControlFlow("else if (_json$L != null)", suffix);
        body.addStatement("_records$L = $T.singletonList(_json$L)", suffix, Collections.class, suffix);
        body.nextControlFlow("else");
        body.addStatement("_records$L = $T.emptyList()", suffix, Collections.class);
        body.endControlFlow();

        // Open the for-loop. Body of the next level (or innermost keys.add) is emitted
        // by the caller into this open scope.
        body.beginControlFlow("for ($T _rec$L : _records$L)", Object.class, suffix, suffix);
        body.beginControlFlow("if (!(_rec$L instanceof $T<?,?> _m$L))", suffix, Map.class, suffix);
        body.addStatement("continue");
        body.endControlFlow();
        body.addStatement("$T _v$L = _m$L.get($S)", Object.class, suffix, suffix, parentKey);
        body.beginControlFlow("if (_v$L == null)", suffix);
        body.addStatement("continue");
        body.endControlFlow();

        // Build _p<level>: copy _p<level-1> if any, then add partition_field=extracted value.
        if (isFirstLevel) {
            body.addStatement("$T _p$L = new $T<>()",
                mapStringString, suffix, LinkedHashMap.class);
        } else {
            int prev = Integer.parseInt(suffix) - 1;
            body.addStatement("$T _p$L = new $T<>(_p$L)",
                mapStringString, suffix, LinkedHashMap.class, prev);
        }
        body.addStatement("_p$L.put($S, $T.valueOf(_v$L))",
            suffix, partitionField, String.class, suffix);
    }

    /**
     * Emits {@code HttpRequest <reqVar> = ...; HttpResponse<String> <respVar> = sendWithRetry(<reqVar>);}
     * with auth headers, custom request headers, and HTTP method matching the requester spec.
     * Uses caller-supplied variable names so multiple requests can coexist in the same scope.
     */
    private void emitAuthedGet(
        CodeBlock.Builder body, AuthenticatorSpec auth, RequesterSpec requester,
        String urlExpr, String reqVar, String respVar
    ) throws CodegenException {
        String httpMethod = requester != null ? requester.getHttpMethod() : "GET";
        boolean jsonBody  = needsJsonBody(requester, null);
        boolean dataBody  = needsDataBody(requester, null);
        String bodyVar    = null;
        if (jsonBody || dataBody) {
            try {
                // Use reqVar as suffix to avoid collisions when called multiple times in one scope.
                bodyVar = buildBodyPreamble(body, requester, null, "_" + reqVar);
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new CodegenException("Failed to serialize request_body_json: " + e.getMessage());
            }
        }
        if (auth != null && auth.isOAuth()) {
            body.addStatement("$T _accessToken_$L = refreshAccessToken()", String.class, reqVar);
        } else if (auth != null && auth.isJwt()) {
            body.addStatement("$T _jwtToken_$L = buildJwt()", String.class, reqVar);
        }
        StringBuilder fmt = new StringBuilder("$T $L = $T.newBuilder()\n        .uri($T.create(normalizeUrl($L)))");
        List<Object> args = new ArrayList<>();
        args.add(HTTP_REQUEST);
        args.add(reqVar);
        args.add(HTTP_REQUEST);
        args.add(URI_CLASS);
        args.add(urlExpr);
        Map<String, String> egHeaders = requester != null ? requester.getRequestHeaders() : Collections.emptyMap();
        boolean egHasExplicitCt = egHeaders.keySet().stream()
            .anyMatch(k -> k.equalsIgnoreCase("Content-Type"));
        boolean egHasExplicitUa = egHeaders.keySet().stream()
            .anyMatch(k -> k.equalsIgnoreCase("User-Agent"));
        boolean egHasExplicitAccept = egHeaders.keySet().stream()
            .anyMatch(k -> k.equalsIgnoreCase("Accept"));
        if (!egHasExplicitUa) {
            fmt.append("\n        .header(\"User-Agent\", \"kafka-connect-airbyte/1.0\")");
        }
        if (!egHasExplicitAccept) {
            fmt.append("\n        .header(\"Accept\", \"application/json\")");
        }
        if (bodyVar != null && !egHasExplicitCt) {
            if (jsonBody) {
                fmt.append("\n        .header(\"Content-Type\", \"application/json\")");
            } else if (dataBody) {
                fmt.append("\n        .header(\"Content-Type\", \"application/x-www-form-urlencoded\")");
            }
        }
        if (auth == null || auth.isNoAuth() || (auth.isApiKey() && isApiKeyQueryParam(auth))) {
            // no header
        } else if (auth.isBearer()) {
            fmt.append("\n        .header(\"Authorization\", \"Bearer \" + $L)");
            args.add(interpolateTemplate(auth.getApiToken()));
        } else if (auth.isApiKey()) {
            fmt.append("\n        .header($S, $L)");
            args.add(resolveApiKeyHeaderName(auth));
            args.add(interpolateTemplate(auth.getApiToken()));
        } else if (auth.isBasicHttp()) {
            fmt.append("\n        .header(\"Authorization\", \"Basic \" + cachedCredentials)");
        } else if (auth.isOAuth()) {
            fmt.append("\n        .header(\"Authorization\", \"Bearer \" + _accessToken_").append(reqVar).append(")");
        } else if (auth.isSessionToken()) {
            fmt.append("\n        .header(\"Authorization\", \"Bearer \" + cachedSessionToken)");
        } else if (auth.isLegacySessionToken()) {
            fmt.append("\n        .header($S, cachedLegacyToken)");
            args.add(auth.getHeader());
        } else if (auth.isJwt()) {
            fmt.append("\n        .header(\"Authorization\", \"$L \" + _jwtToken_").append(reqVar).append(")");
            args.add(auth.getHeaderPrefix());
        }
        for (Map.Entry<String, String> h : egHeaders.entrySet()) {
            fmt.append("\n        .header($S, $L)");
            args.add(h.getKey());
            args.add(interpolateTemplate(h.getValue()));
        }
        appendHttpMethod(fmt, args, httpMethod, jsonBody, dataBody, bodyVar);
        fmt.append("\n        .build()");
        body.addStatement(fmt.toString(), args.toArray());
        emitSendWithRetryCall(body, respVar, reqVar,
            requester != null ? requester.getErrorHandler() : null);
    }

    /**
     * Generates the poll method for a child stream with multiple (nested)
     * SubstreamPartitionRouters. Consumes one {@code Map<String,String>} partition
     * entry per call from the cached {@code <stream>_partitionKeys} field.
     */
    private MethodSpec buildNestedSubstreamPollMethod(
        StreamSpec stream,
        ClassName configClass,
        AuthenticatorSpec auth,
        ParameterizedTypeName listOfSourceRecord
    ) throws CodegenException {
        RequesterSpec requester = stream.getRetriever().getRequester();
        String baseUrl = requester.effectiveBaseUrl();
        String rawPath = requester.getPath();
        if (!baseUrl.isEmpty() && !baseUrl.endsWith("/") && !rawPath.isEmpty() && !rawPath.startsWith("/")) {
            baseUrl = baseUrl + "/";
        }
        String fullUrlTemplate = joinUrl(baseUrl, rawPath);
        String streamName = stream.getName();
        String methodName = "poll" + ManifestSpec.toClassName(streamName);
        List<String> fieldPath = extractFieldPath(stream);

        String keysField = partitionKeysFieldName(streamName);
        String idxField  = partitionIdxFieldName(streamName);

        ParameterizedTypeName mapStringString = ParameterizedTypeName.get(
            ClassName.get("java.util", "Map"),
            ClassName.get(String.class), ClassName.get(String.class));
        ParameterizedTypeName mapStringObject = ParameterizedTypeName.get(
            ClassName.get("java.util", "Map"),
            ClassName.get(String.class), ClassName.get(Object.class));

        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("final $T streamName = $S", String.class, streamName);
        body.addStatement("$T<$T> result = new $T<>()", List.class, SOURCE_RECORD, ArrayList.class);

        // Restore partition index from offset store on first poll.
        body.beginControlFlow("if ($L < 0)", idxField);
        body.addStatement(
            "$T<$T, $T> _stored = context.offsetStorageReader().offset($T.of($S, streamName))",
            Map.class, String.class, Object.class, Map.class, "stream");
        body.addStatement("$L = 0", idxField);
        body.beginControlFlow(
            "if (_stored != null && _stored.get($S) instanceof $T _p)", "partition_idx", Number.class);
        body.addStatement("$L = _p.intValue()", idxField);
        body.endControlFlow();
        body.endControlFlow();

        body.beginControlFlow("if ($L == null || $L.isEmpty())", keysField, keysField);
        body.addStatement("return result");
        body.endControlFlow();

        body.beginControlFlow("if ($L >= $L.size())", idxField, keysField);
        body.addStatement("$L = 0", idxField);
        body.beginControlFlow("try");
        body.addStatement("$L = fetch$LPartitionKeys()", keysField, ManifestSpec.toClassName(streamName));
        body.nextControlFlow("catch ($T _e)", Exception.class);
        body.endControlFlow();
        body.beginControlFlow("if ($L == null || $L.isEmpty())", keysField, keysField);
        body.addStatement("return result");
        body.endControlFlow();
        body.endControlFlow();

        body.addStatement("$T _partition = $L.get($L)", mapStringString, keysField, idxField);
        body.addStatement("int _nextIdx = ($L + 1 >= $L.size()) ? 0 : $L + 1",
            idxField, keysField, idxField);

        // Build URL via JinjaRenderer with stream_partition exposed.
        body.addStatement("$T _ctx = jinjaCtx()", mapStringObject);
        body.addStatement("_ctx.put($S, _partition)", "stream_partition");
        body.addStatement("$T urlBuilder = new $T(render($S, _ctx))",
            StringBuilder.class, StringBuilder.class, fullUrlTemplate);

        body.beginControlFlow("try");
        buildRequestStatement(body, auth, null, requester);
        emitSendWithRetryCall(body, "response", "request", requester.getErrorHandler());

        // 4xx/5xx: skip + advance (don't crash the connector).
        body.beginControlFlow("if (response.statusCode() < 200 || response.statusCode() >= 300)");
        body.addStatement("$L = _nextIdx", idxField);
        body.addStatement("return result");
        body.endControlFlow();
        body.beginControlFlow("if (response.body() == null || response.body().isBlank())");
        body.addStatement("$L = _nextIdx", idxField);
        body.addStatement("return result");
        body.endControlFlow();

        body.addStatement("$T json = MAPPER.readValue(response.body(), $T.class)",
            Object.class, Object.class);
        for (String seg : fieldPath) {
            body.beginControlFlow("if (json instanceof $T<?,?> _fpm)", Map.class);
            body.addStatement("json = _fpm.get($S)", seg);
            body.endControlFlow();
        }

        body.addStatement("$T<$T> records", List.class, Object.class);
        body.beginControlFlow("if (json instanceof $T)", List.class);
        body.addStatement("records = ($T<$T>) json", List.class, Object.class);
        body.nextControlFlow("else if (json != null)");
        body.addStatement("records = $T.singletonList(json)", Collections.class);
        body.nextControlFlow("else");
        body.addStatement("records = $T.emptyList()", Collections.class);
        body.endControlFlow();

        CodeBlock nestedPositionMap = CodeBlock.of("$T.of($S, _nextIdx)", Map.class, "partition_idx");
        body.beginControlFlow("for ($T record : records)", Object.class);
        body.add(buildPipelineEmitBlock(pipelineFieldName(streamName), nestedPositionMap));
        body.endControlFlow();

        body.addStatement("$L = _nextIdx", idxField);

        body.nextControlFlow("catch ($T e)", InterruptedException.class);
        body.addStatement("$T.currentThread().interrupt()", Thread.class);
        body.addStatement("throw new $T(\"Interrupted while polling \" + streamName, e)", CONNECT_EXCEPTION);
        body.nextControlFlow("catch ($T e)", Exception.class);
        body.addStatement("throw new $T(\"Failed to poll \" + streamName, e)", CONNECT_EXCEPTION);
        body.endControlFlow();

        body.addStatement("return result");

        return MethodSpec.methodBuilder(methodName)
            .addModifiers(Modifier.PRIVATE)
            .returns(listOfSourceRecord)
            .addException(InterruptedException.class)
            .addCode(body.build())
            .build();
    }
}
