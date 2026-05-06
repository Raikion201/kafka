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
import org.apache.kafka.connect.manifest.codegen.model.IncrementalSyncSpec;
import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;
import org.apache.kafka.connect.manifest.codegen.model.PaginatorSpec;
import org.apache.kafka.connect.manifest.codegen.model.PartitionRouterSpec;
import org.apache.kafka.connect.manifest.codegen.model.RecordSelectorSpec;
import org.apache.kafka.connect.manifest.codegen.model.RequesterSpec;
import org.apache.kafka.connect.manifest.codegen.model.StreamSpec;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;

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

    private static final String APP_VERSION = "1.0.0";

    /** Keys declared in the current manifest's spec.connection_specification.properties.
     *  Set at the start of {@link #generate} so credential resolution can fall back to
     *  {@code ""} for Airbyte sentinel keys like {@code nothing} that have no real property. */
    private java.util.Set<String> currentSpecPropKeys = java.util.Collections.emptySet();

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

        typeBuilder.addMethod(buildStart(mapStringString, configClass, auth, runnableStreams));

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

        return JavaFile.builder(pkgName, typeBuilder.build())
            .skipJavaLangImports(true)
            .addStaticImport(JINJA_RENDERER, "render")
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
        typeBuilder.addField(HTTP_CLIENT, "httpClient", Modifier.PRIVATE);

        boolean hasHttpStream = streams.stream().anyMatch(s -> !isCustomDispatch(s));
        if (hasHttpStream) {
            typeBuilder.addField(RETRY_POLICY, "retryPolicy", Modifier.PRIVATE);
            typeBuilder.addField(BACKOFF_STRATEGY, "backoffStrategy", Modifier.PRIVATE);
        }

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
        List<StreamSpec> streams
    ) {
        MethodSpec.Builder m = MethodSpec.methodBuilder("start")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(mapStringString, "props")
            .addStatement("this.config = new $T(props)", configClass)
            .addStatement("this.httpClient = $T.newHttpClient()", HTTP_CLIENT);

        StreamSpec primaryHttpStream = streams.stream()
            .filter(s -> !isCustomDispatch(s))
            .findFirst()
            .orElse(null);
        if (primaryHttpStream != null) {
            RequesterSpec.ErrorHandlerSpec eh = primaryHttpStream.getRetriever().getRequester().getErrorHandler();
            m.addStatement("this.retryPolicy = $L", retryPolicyExpr(eh));
            m.addStatement("this.backoffStrategy = $L", backoffChainExpr(eh));
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
            body.addStatement("all.addAll($L())", pollMethod);
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
        if (!baseUrl.isEmpty() && !baseUrl.endsWith("/") && !path.isEmpty() && !path.startsWith("/")) {
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
            body.add(buildPaginationInit(paginator));
        } else {
            body.addStatement("$T<$T> result = new $T<>()", List.class, SOURCE_RECORD, ArrayList.class);
        }

        // Restore or initialise DatetimeBasedCursor from offset store.
        if (isIncremental) {
            body.add(buildIncrementalInit(streamName, incrementalSync));
        }

        body.add(buildUrlBlock(baseUrl, path, requestParams, paginator, hasPagination, auth,
            incrementalSync, cursorVar));
        body.beginControlFlow("try");
        buildRequestStatement(body, auth, paginator);
        body.add(buildFetchBlock(paginator));
        if (hasPagination && paginator.isCursor()) {
            body.add(buildCursorStateUpdate(paginator));
        }
        body.add(buildFieldPathNav(fieldPath, hasPagination));
        body.add(buildNormalizeAndCollect(hasPagination, paginator, cursorVar,
            isIncremental ? incrementalSync.getCursorField() : null));
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
        body.add(buildSubstreamUrlBlock(baseUrl, rawPath, paginator, hasPagination));

        body.beginControlFlow("try");
        buildRequestStatement(body, auth, paginator);
        body.add(buildFetchBlock(paginator));
        if (hasPagination && paginator.isCursor()) {
            body.add(buildCursorStateUpdate(paginator));
        }
        body.add(buildFieldPathNav(fieldPath, hasPagination));
        body.add(buildNormalizeAndCollect(hasPagination, paginator, null, null));

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
        PaginatorSpec paginator, boolean hasPagination
    ) {
        CodeBlock.Builder b = CodeBlock.builder();

        // Compute the base URL with partition variable substituted.
        Matcher m = STREAM_PARTITION_RE.matcher(rawPath);
        boolean noPartitionVar = !m.find();

        if (isRequestPath(paginator)) {
            // For RequestPath cursor, the cursor itself is the full URL for page 2+.
            // For the first page, build the URL from base + path.
            if (noPartitionVar) {
                b.addStatement("$T url = (nextCursor != null) ? nextCursor : $S",
                    String.class, baseUrl + rawPath);
            } else {
                String before = rawPath.substring(0, m.start());
                String after  = rawPath.substring(m.end());
                b.addStatement(
                    "$T _baseUrl = $S + $T.encode(_partitionKey, $T.UTF_8) + $S",
                    String.class, baseUrl + before,
                    ClassName.get("java.net", "URLEncoder"),
                    ClassName.get("java.nio.charset", "StandardCharsets"),
                    after);
                b.addStatement("$T url = (nextCursor != null) ? nextCursor : _baseUrl", String.class);
            }
            return b.build();
        }

        // Standard urlBuilder path.
        if (noPartitionVar) {
            b.addStatement("$T urlBuilder = new $T($S)",
                StringBuilder.class, StringBuilder.class, baseUrl + rawPath);
        } else {
            String before = rawPath.substring(0, m.start());
            String after  = rawPath.substring(m.end());
            b.addStatement(
                "$T urlBuilder = new $T($S + $T.encode(_partitionKey, $T.UTF_8) + $S)",
                StringBuilder.class, StringBuilder.class,
                baseUrl + before,
                ClassName.get("java.net", "URLEncoder"),
                ClassName.get("java.nio.charset", "StandardCharsets"),
                after);
        }
        if (hasPagination && paginator != null) {
            appendPaginationParams(b, paginator, !(baseUrl + rawPath).contains("?"));
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
        String baseUrl = parentRequester.effectiveBaseUrl();
        String path    = parentRequester.getPath();
        if (!baseUrl.isEmpty() && !baseUrl.endsWith("/") && !path.isEmpty() && !path.startsWith("/")) {
            baseUrl = baseUrl + "/";
        }
        PaginatorSpec parentPaginator = parentStream.getRetriever().getPaginator();
        List<String> fieldPath = extractFieldPath(parentStream);

        ParameterizedTypeName arrayListString = ParameterizedTypeName.get(
            ClassName.get("java.util", "ArrayList"), ClassName.get(String.class));

        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("$T keys = new $T()", listString, arrayListString);
        body.addStatement("$T nextCursor = null", String.class);

        body.beginControlFlow("do");
        body.addStatement("$T urlBuilder = new $T($S)", StringBuilder.class, StringBuilder.class, baseUrl + path);

        // Append cursor pagination token if present (not RequestPath).
        if (parentPaginator != null && parentPaginator.isCursor()
                && parentPaginator.getPageTokenOption() != null
                && !parentPaginator.getPageTokenOption().isRequestPath()) {
            body.beginControlFlow("if (nextCursor != null && !nextCursor.isEmpty())");
            body.addStatement("urlBuilder.append($S + nextCursor)", "?" + parentPaginator.pageParamName() + "=");
            body.endControlFlow();
        }

        // Build and send request (same auth as child stream).
        buildRequestStatement(body, auth, null);
        body.addStatement("$T<$T> response = sendWithRetry(request)", HTTP_RESPONSE, String.class);
        body.beginControlFlow("if (response.statusCode() < 200 || response.statusCode() >= 300)");
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
            StringBuilder.class, StringBuilder.class, baseUrl + pathPrefix,
            ClassName.get("java.net", "URLEncoder"),
            ClassName.get("java.nio.charset", "StandardCharsets"));

        // Request parameters: list-cycle ones use _item; normal config templates use getter
        boolean firstParam = !(baseUrl + pathPrefix).contains("?");
        Pattern listPat = Pattern.compile("config\\['" + listCycleField + "'\\]\\.split");
        for (Map.Entry<String, String> entry : requestParams.entrySet()) {
            String sep = firstParam ? "?" : "&";
            if (listPat.matcher(entry.getValue()).find()) {
                body.addStatement(
                    "urlBuilder.append($S + $T.encode(_item, $T.UTF_8))",
                    sep + entry.getKey() + "=",
                    ClassName.get("java.net", "URLEncoder"),
                    ClassName.get("java.nio.charset", "StandardCharsets"));
            } else {
                String tmpl = entry.getValue();
                if (tmpl != null && (tmpl.contains("{{") || tmpl.contains("{%"))) {
                    body.addStatement(
                        "urlBuilder.append($S + $T.encode($T.valueOf($L), $T.UTF_8))",
                        sep + entry.getKey() + "=",
                        ClassName.get("java.net", "URLEncoder"),
                        ClassName.get(String.class), interpolateTemplate(tmpl),
                        ClassName.get("java.nio.charset", "StandardCharsets"));
                }
                // skip params with empty / unresolvable values
            }
            firstParam = false;
        }

        // Build HTTP request with optional auth and custom static headers
        body.beginControlFlow("try");
        StringBuilder reqFmt = new StringBuilder(
            "$T request = $T.newBuilder()\n        .uri($T.create(urlBuilder.toString()))");
        List<Object> reqArgs = new ArrayList<>();
        reqArgs.add(HTTP_REQUEST);
        reqArgs.add(HTTP_REQUEST);
        reqArgs.add(URI_CLASS);
        if (auth != null && auth.isBearer()) {
            reqFmt.append("\n        .header(\"Authorization\", \"Bearer \" + $L)");
            reqArgs.add(interpolateTemplate(auth.getApiToken()));
        } else if (auth != null && auth.isBasicHttp()) {
            reqFmt.append("\n        .header(\"Authorization\", \"Basic \" + cachedCredentials)");
        }
        for (Map.Entry<String, String> h : requestHeaders.entrySet()) {
            reqFmt.append("\n        .header($S, $S)");
            reqArgs.add(h.getKey());
            reqArgs.add(h.getValue());
        }
        reqFmt.append("\n        .GET()\n        .build()");
        body.addStatement(reqFmt.toString(), reqArgs.toArray());

        // Fetch with retry
        body.addStatement("$T<$T> response = sendWithRetry(request)", HTTP_RESPONSE, String.class);

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
        body.addStatement("$T json = MAPPER.readValue(response.body(), $T.class)", Object.class, Object.class);

        if (!fieldPath.isEmpty()) {
            body.add(buildFieldPathNav(fieldPath, true));
        }

        body.addStatement("$T<$T> records", List.class, Object.class);
        body.beginControlFlow("if (json instanceof $T)", List.class);
        body.addStatement("records = ($T<$T>) json", List.class, Object.class);
        body.nextControlFlow("else");
        body.addStatement("records = $T.singletonList(json)", Collections.class);
        body.endControlFlow();

        body.beginControlFlow("for ($T record : records)", Object.class);
        body.addStatement("$T value = MAPPER.writeValueAsString(record)", String.class);
        body.addStatement(
            "result.add(new $T($T.of($S, streamName), $T.of($S, _nextIndex), streamName, $T.STRING_SCHEMA, value))",
            SOURCE_RECORD, Map.class, "stream", Map.class, "ticker_index", SCHEMA);
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
            b.addStatement("$L = $L", cursorVar, interpolateTemplate(startDt.getDatetime()));
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

    private CodeBlock buildPaginationInit(PaginatorSpec paginator) {
        CodeBlock.Builder b = CodeBlock.builder();
        b.addStatement("$T<$T> result = new $T<>()", List.class, SOURCE_RECORD, ArrayList.class);
        // Read stored offset so restarts resume from the last committed position.
        b.addStatement(
            "$T<$T, $T> _stored = context.offsetStorageReader().offset($T.of($S, streamName))",
            Map.class, String.class, Object.class, Map.class, "stream");
        if (paginator.isCursor()) {
            b.addStatement("$T nextCursor = null", String.class);
            b.beginControlFlow("if (_stored != null && _stored.get($S) instanceof $T _c && !_c.isEmpty())",
                "cursor", String.class);
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
        String cursorVarName
    ) {
        CodeBlock.Builder b = CodeBlock.builder();

        if (isRequestPath(paginator)) {
            String initialUrl = baseUrl + path;
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
        boolean pathHasQuery = (baseUrl + path).contains("?");

        List<String> paramKeys = new ArrayList<>();
        for (Map.Entry<String, String> entry : requestParams.entrySet()) {
            String tmpl = entry.getValue();
            if (tmpl == null) continue;
            if (tmpl.contains("{{") || tmpl.contains("{%")) {
                paramKeys.add(entry.getKey());
                boolean firstOfGroup = paramKeys.size() == 1;
                String sep = (pathHasQuery || !firstOfGroup) ? "&" : "?";
                b.addStatement("urlBuilder.append($S + $T.encode($T.valueOf($L), $T.UTF_8))",
                    sep + entry.getKey() + "=",
                    ClassName.get("java.net", "URLEncoder"),
                    ClassName.get(String.class), interpolateTemplate(tmpl),
                    ClassName.get("java.nio.charset", "StandardCharsets"));
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

        // DatetimeBasedCursor: inject start / end date range as query params
        if (incrementalSync != null && incrementalSync.isDatetimeBased()) {
            hasParams = appendIncrementalSyncParams(b, incrementalSync, hasParams, cursorVarName);
        }

        if (!hasPagination || paginator == null) return b.build();
        appendPaginationParams(b, paginator, !hasParams);
        return b.build();
    }

    /**
     * Initializes {@code urlBuilder} for the stream poll method.
     * When the base URL or path contains a Jinja template, the initializer is built via
     * {@link JinjaSnippets#interpolateTemplate} so the value is rendered through
     * {@code JinjaRenderer.render(...)} at runtime — same path used for auth headers and cursors.
     */
    private void addInitialUrlStatement(CodeBlock.Builder b, String baseUrl, String path) {
        String combined = baseUrl + path;
        if (!combined.contains("{{") && !combined.contains("{%")) {
            b.addStatement("$T urlBuilder = new $T($S)", StringBuilder.class, StringBuilder.class,
                combined);
            return;
        }
        b.addStatement("$T urlBuilder = new $T($L)",
            StringBuilder.class, StringBuilder.class, interpolateTemplate(combined));
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
        CodeBlock.Builder b, IncrementalSyncSpec sync, boolean hasParams, String cursorVarName
    ) {
        IncrementalSyncSpec.TimeOptionSpec startOpt = sync.getStartTimeOption();
        if (startOpt != null && startOpt.getFieldName() != null) {
            String sep = hasParams ? "&" : "?";
            if (cursorVarName != null) {
                // Use the in-memory cursor variable (epoch seconds or ISO string).
                b.addStatement("urlBuilder.append($S + $T.encode($L, $T.UTF_8))",
                    sep + startOpt.getFieldName() + "=",
                    ClassName.get("java.net", "URLEncoder"), cursorVarName,
                    ClassName.get("java.nio.charset", "StandardCharsets"));
            } else {
                IncrementalSyncSpec.DatetimeSpec startDt = sync.getStartDatetime();
                if (startDt != null && startDt.getDatetime() != null) {
                    b.addStatement("urlBuilder.append($S + $T.encode($T.valueOf($L), $T.UTF_8))",
                        sep + startOpt.getFieldName() + "=",
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
            // Epoch seconds — many APIs (e.g. Delighted) expect integer timestamps.
            b.addStatement("urlBuilder.append($S + $T.valueOf($T.currentTimeMillis() / 1000))",
                sep + endOpt.getFieldName() + "=",
                String.class, System.class);
            hasParams = true;
        }
        return hasParams;
    }

    private void appendPaginationParams(
        CodeBlock.Builder b, PaginatorSpec paginator, boolean firstParam
    ) {
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
            b.addStatement("urlBuilder.append($S + offset)", sep + paginator.pageParamName() + "=");
            b.addStatement("urlBuilder.append($S + pageLimit)", "&" + paginator.sizeParamName() + "=");
        }
    }

    private CodeBlock buildFetchBlock(PaginatorSpec paginator) {
        String urlRef = isRequestPath(paginator) ? "url" : "urlBuilder";
        CodeBlock.Builder b = CodeBlock.builder();
        b.addStatement("$T<$T> response = sendWithRetry(request)", HTTP_RESPONSE, String.class);
        b.beginControlFlow("if (response.statusCode() < 200 || response.statusCode() >= 300)");
        b.addStatement(
            "throw new $T(\"HTTP \" + response.statusCode() + \" from \" + " + urlRef + ")",
            CONNECT_EXCEPTION);
        b.endControlFlow();
        b.addStatement("$T json = MAPPER.readValue(response.body(), $T.class)", Object.class, Object.class);
        return b.build();
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
        boolean hasPagination, PaginatorSpec paginator, String cursorVarName, String jsonCursorField
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

        // For DatetimeBasedCursor: scan records for the max cursor value BEFORE emitting
        // SourceRecords so that the offset stored in each record already reflects the
        // most advanced position in this batch.
        if (cursorVarName != null && jsonCursorField != null) {
            b.add(buildIncrementalCursorUpdate(cursorVarName, jsonCursorField));
        }

        CodeBlock positionMap = buildPositionMapCode(paginator, cursorVarName);
        b.beginControlFlow("for ($T record : records)", Object.class);
        b.addStatement("$T value = MAPPER.writeValueAsString(record)", String.class);
        b.addStatement(
            "result.add(new $T($T.of(\"stream\", streamName), $L, streamName, $T.STRING_SCHEMA, value))",
            SOURCE_RECORD, Map.class, positionMap, SCHEMA);
        b.endControlFlow();
        return b.build();
    }

    private CodeBlock buildPositionMapCode(PaginatorSpec paginator, String cursorVarName) {
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

    /** Generates the {@code HttpRequest.newBuilder()...build()} statement with auth headers. */
    private void buildRequestStatement(CodeBlock.Builder body, AuthenticatorSpec auth, PaginatorSpec paginator) {
        if (auth == null || auth.isNoAuth()) {
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create($L))\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, urlExpr(auth, paginator)
            );
        } else if (auth.isBearer()) {
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create($L))\n"
                    + "        .header(\"Authorization\", \"Bearer \" + $L)\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, urlExpr(auth, paginator),
                interpolateTemplate(auth.getApiToken())
            );
        } else if (auth.isApiKey() && isApiKeyQueryParam(auth)) {
            // key already appended to URL as query param — no auth header needed
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create($L))\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, urlExpr(auth, paginator)
            );
        } else if (auth.isApiKey()) {
            String headerName = resolveApiKeyHeaderName(auth);
            String headerValueExpr = interpolateTemplate(auth.getApiToken());
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create($L))\n"
                    + "        .header($S, $L)\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, urlExpr(auth, paginator), headerName, headerValueExpr
            );
        } else if (auth.isBasicHttp()) {
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create($L))\n"
                    + "        .header(\"Authorization\", \"Basic \" + cachedCredentials)\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, urlExpr(auth, paginator)
            );
        } else if (auth.isOAuth()) {
            body.addStatement("$T accessToken = refreshAccessToken()", String.class);
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create($L))\n"
                    + "        .header(\"Authorization\", \"Bearer \" + accessToken)\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, urlExpr(auth, paginator)
            );
        } else if (auth.isSessionToken()) {
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create($L))\n"
                    + "        .header(\"Authorization\", \"Bearer \" + cachedSessionToken)\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, urlExpr(auth, paginator)
            );
        } else if (auth.isLegacySessionToken()) {
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create($L))\n"
                    + "        .header($S, cachedLegacyToken)\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, urlExpr(auth, paginator), auth.getHeader()
            );
        } else if (auth.isJwt()) {
            body.addStatement("$T jwtToken = buildJwt()", String.class);
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create($L))\n"
                    + "        .header(\"Authorization\", \"$L \" + jwtToken)\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, urlExpr(auth, paginator), auth.getHeaderPrefix()
            );
        } else {
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create($L))\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, urlExpr(auth, paginator)
            );
        }
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
        String endpoint = auth.getTokenRefreshEndpoint() != null ? auth.getTokenRefreshEndpoint() : "";

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
                + "        .uri($T.create($S))\n"
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
        String urlBase = login.getUrlBase() == null ? "" : login.getUrlBase();
        String loginUrlExpr = interpolateTemplate(urlBase + "/" + login.getPath());

        // Build JSON body string from requestBodyJson
        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("$T<$T, $T> bodyMap = new $T<>()", Map.class, String.class, String.class,
            ClassName.get("java.util", "LinkedHashMap"));
        if (login.getRequestBodyJson() != null) {
            for (Map.Entry<String, String> e : login.getRequestBodyJson().entrySet()) {
                body.addStatement("bodyMap.put($S, $L)", e.getKey(), interpolateTemplate(e.getValue()));
            }
        }
        body.addStatement("$T loginBody = MAPPER.writeValueAsString(bodyMap)", String.class);

        // Build the request
        ClassName bodyPublishers = ClassName.get("java.net.http", "HttpRequest.BodyPublishers");
        StringBuilder reqBuilder = new StringBuilder(
            "$T loginReq = $T.newBuilder()\n"
                + "        .uri($T.create(" + loginUrlExpr + "))\n"
                + "        .header(\"Content-Type\", \"application/json\")\n");

        List<Object> reqArgs = new ArrayList<>();
        reqArgs.add(HTTP_REQUEST);
        reqArgs.add(HTTP_REQUEST);
        reqArgs.add(URI_CLASS);

        // Add Basic auth header if the inner authenticator is BasicHttp
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
        }
        reqBuilder.append("        .POST($T.ofString(loginBody))\n        .build()");
        reqArgs.add(bodyPublishers);

        body.addStatement(reqBuilder.toString(), reqArgs.toArray());
        body.addStatement(
            "$T<$T> loginResp = httpClient.send(loginReq, $T.BodyHandlers.ofString())",
            HTTP_RESPONSE, String.class, HTTP_RESPONSE);
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
        body.addStatement("throw new $T(\"Session token not found in login response\")", CONNECT_EXCEPTION);
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
            body.addStatement("$T _value = MAPPER.writeValueAsString(_record)", String.class);
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
            .addException(Exception.class)
            .addCode(body.build())
            .build();
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
        return MethodSpec.methodBuilder("jinjaCtx")
            .addModifiers(Modifier.PRIVATE)
            .returns(mapStringObject)
            .addStatement("$T ctx = new $T<>()", mapStringObject, linkedHashMap)
            .beginControlFlow("if (config != null)")
            .addStatement("ctx.put($S, config.originalsStrings())", "config")
            .endControlFlow()
            .addStatement("return ctx")
            .build();
    }

    private MethodSpec buildStop() {
        return MethodSpec.methodBuilder("stop")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .beginControlFlow("if (httpClient instanceof $T ac)", AutoCloseable.class)
            .beginControlFlow("try")
            .addStatement("ac.close()")
            .nextControlFlow("catch ($T ignored)", Exception.class)
            .endControlFlow()
            .endControlFlow()
            .addStatement("httpClient = null")
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
        return fp == null ? Collections.emptyList() : fp;
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

    /**
     * Generates a Java variable name for a ListPartitionRouter loop variable.
     * e.g. cursorField="breakdown" → "_lp_breakdown"; null → "_lp_partition".
     */
    private static String listLoopVar(String cursorField) {
        return "_lp_" + (cursorField != null ? cursorField : "partition").replaceAll("[^a-zA-Z0-9]", "_");
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

        body.add(buildListRouterUrlBlock(baseUrl, rawPath, listRouters, paginator, hasPagination));

        body.beginControlFlow("try");
        buildRequestStatement(body, auth, paginator);
        body.add(buildFetchBlock(paginator));
        if (hasPagination && paginator.isCursor()) {
            body.add(buildCursorStateUpdate(paginator));
        }
        body.add(buildFieldPathNav(fieldPath, hasPagination));
        body.add(buildNormalizeAndCollect(hasPagination, paginator, null, null));
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
            "\\{\\{\\s*stream_partition(?:\\." + q
            + "|\\[\\s*['\"]" + q + "['\"]\\s*\\])\\s*\\}\\}");
    }

    /**
     * Builds the URL construction block for a stream with one or more ListPartitionRouters.
     * Substitutes stream_partition.X in the path with the matching loop variable,
     * and appends query params from request_option.inject_into=request_parameter.
     */
    private CodeBlock buildListRouterUrlBlock(
        String baseUrl, String rawPath,
        List<PartitionRouterSpec> listRouters,
        PaginatorSpec paginator, boolean hasPagination
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
            boolean needsPlus = appendUrlSegment(fmt, fmtArgs, baseUrl + before, false);
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

        if (hasPagination && paginator != null) {
            appendPaginationParams(b, paginator, !rawPath.contains("?"));
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
    ) {
        RequesterSpec parentRequester = parentStream.getRetriever().getRequester();
        String baseUrl = parentRequester.effectiveBaseUrl();
        String path = parentRequester.getPath();
        if (!baseUrl.isEmpty() && !baseUrl.endsWith("/") && !path.isEmpty() && !path.startsWith("/")) {
            baseUrl = baseUrl + "/";
        }
        String fullUrlTemplate = baseUrl + path;
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
        emitAuthedGet(body, auth, "_url" + suffix, "_req" + suffix, "_resp" + suffix);

        // Skip branch on non-2xx: continue if not first-level (we're inside a for-loop),
        // else return whatever we have so far.
        body.beginControlFlow("if (_resp$L.statusCode() < 200 || _resp$L.statusCode() >= 300)",
            suffix, suffix);
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
     * with auth headers matching {@code auth}. Mirrors the auth branches in
     * {@link #buildRequestStatement} but uses caller-supplied variable names so
     * multiple requests can coexist in the same scope.
     */
    private void emitAuthedGet(
        CodeBlock.Builder body, AuthenticatorSpec auth,
        String urlExpr, String reqVar, String respVar
    ) {
        if (auth != null && auth.isOAuth()) {
            body.addStatement("$T _accessToken_$L = refreshAccessToken()", String.class, reqVar);
        }
        StringBuilder fmt = new StringBuilder("$T $L = $T.newBuilder()\n        .uri($T.create($L))");
        List<Object> args = new ArrayList<>();
        args.add(HTTP_REQUEST);
        args.add(reqVar);
        args.add(HTTP_REQUEST);
        args.add(URI_CLASS);
        args.add(urlExpr);
        if (auth == null || auth.isNoAuth()) {
            // no header
        } else if (auth.isBearer()) {
            fmt.append("\n        .header(\"Authorization\", \"Bearer \" + $L)");
            args.add(interpolateTemplate(auth.getApiToken()));
        } else if (auth.isApiKey() && !isApiKeyQueryParam(auth)) {
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
        }
        fmt.append("\n        .GET()\n        .build()");
        body.addStatement(fmt.toString(), args.toArray());
        body.addStatement("$T<$T> $L = sendWithRetry($L)",
            HTTP_RESPONSE, String.class, respVar, reqVar);
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
        String fullUrlTemplate = baseUrl + rawPath;
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
        buildRequestStatement(body, auth, null);
        body.addStatement("$T<$T> response = sendWithRetry(request)", HTTP_RESPONSE, String.class);

        // 4xx/5xx: skip + advance (don't crash the connector).
        body.beginControlFlow("if (response.statusCode() < 200 || response.statusCode() >= 300)");
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

        body.beginControlFlow("for ($T record : records)", Object.class);
        body.addStatement("$T value = MAPPER.writeValueAsString(record)", String.class);
        body.addStatement(
            "result.add(new $T($T.of($S, streamName), $T.of($S, _nextIdx), streamName, $T.STRING_SCHEMA, value))",
            SOURCE_RECORD, Map.class, "stream", Map.class, "partition_idx", SCHEMA);
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
