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
import org.apache.kafka.connect.manifest.codegen.model.PaginatorSpec;
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
import java.util.List;
import java.util.Map;
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
 * </ul>
 */
public class TaskGenerator {

    /** Matches Airbyte config-interpolation templates like {@code {{ config['key'] }}} or {@code {{ config["key"] }}}. */
    private static final Pattern CONFIG_TEMPLATE = Pattern.compile(
        "\\{\\{\\s*config\\[['\"]([^'\"]+)['\"]\\]\\s*\\}\\}");

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

    private static final String APP_VERSION = "1.0.0";

    /**
     * Generate the source task class source file.
     *
     * @param spec    the parsed manifest
     * @param pkgName target Java package
     * @return a {@link JavaFile} ready to be written to disk
     * @throws CodegenException if the spec is malformed or a stream has no retriever
     */
    public JavaFile generate(ManifestSpec spec, String pkgName) throws CodegenException {
        List<StreamSpec> streams = spec.resolvedStreams();
        if (streams.isEmpty()) {
            throw new CodegenException("Manifest has no resolved streams; cannot generate task");
        }
        for (StreamSpec stream : streams) {
            if (stream.getRetriever() == null) {
                throw new CodegenException("Stream '" + stream.getName() + "' has no retriever");
            }
            if (stream.getRetriever().getRequester() == null) {
                throw new CodegenException("Stream '" + stream.getName() + "' retriever has no requester");
            }
        }

        String baseName = ManifestSpec.toClassName(
            spec.connectorClassName().replace("Source", ""));
        String taskClassName = baseName + "SourceTask";
        String configClassName = baseName + "ConnectorConfig";
        ClassName configClass = ClassName.get(pkgName, configClassName);

        AuthenticatorSpec auth = streams.get(0).getRetriever().getRequester().getAuthenticator();

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

        addClassFields(typeBuilder, configClass, auth);

        typeBuilder.addMethod(
            MethodSpec.methodBuilder("version")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addStatement("return $S", APP_VERSION)
                .build()
        );

        typeBuilder.addMethod(buildStart(mapStringString, configClass, auth));
        typeBuilder.addMethod(buildPollAll(streams, listOfSourceRecord));

        for (StreamSpec stream : streams) {
            typeBuilder.addMethod(buildStreamPollMethod(stream, configClass, auth, listOfSourceRecord));
        }

        if (auth != null && auth.isOAuth()) {
            typeBuilder.addMethod(buildRefreshAccessToken(configClass, auth));
        }

        typeBuilder.addMethod(buildSendWithRetry());
        typeBuilder.addMethod(buildStop());

        return JavaFile.builder(pkgName, typeBuilder.build())
            .skipJavaLangImports(true)
            .build();
    }

    private void addClassFields(TypeSpec.Builder typeBuilder, ClassName configClass, AuthenticatorSpec auth) {
        typeBuilder.addField(
            FieldSpec.builder(OBJECT_MAPPER, "MAPPER", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer("new $T()", OBJECT_MAPPER)
                .build()
        );
        typeBuilder.addField(configClass, "config", Modifier.PRIVATE);
        typeBuilder.addField(HTTP_CLIENT, "httpClient", Modifier.PRIVATE);

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
    }

    private MethodSpec buildStart(
        ParameterizedTypeName mapStringString,
        ClassName configClass,
        AuthenticatorSpec auth
    ) {
        MethodSpec.Builder m = MethodSpec.methodBuilder("start")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(mapStringString, "props")
            .addStatement("this.config = new $T(props)", configClass)
            .addStatement("this.httpClient = $T.newHttpClient()", HTTP_CLIENT);

        if (auth != null && auth.isBasicHttp()) {
            String userGetter = resolveConfigGetter(auth.getUsername());
            String passGetter = resolveConfigGetter(auth.getPassword());
            m.addStatement(
                "this.cachedCredentials = $T.getEncoder().encodeToString(\n"
                    + "        (config.$L() + \":\" + config.$L()).getBytes($T.UTF_8))",
                ClassName.get("java.util", "Base64"),
                userGetter, passGetter,
                ClassName.get("java.nio.charset", "StandardCharsets")
            );
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

    private MethodSpec buildStreamPollMethod(
        StreamSpec stream,
        ClassName configClass,
        AuthenticatorSpec auth,
        ParameterizedTypeName listOfSourceRecord
    ) throws CodegenException {
        RequesterSpec requester = stream.getRetriever().getRequester();
        String baseUrl = requester.effectiveBaseUrl();
        String path = requester.getPath();
        String streamName = stream.getName();
        PaginatorSpec paginator = stream.getRetriever().getPaginator();
        Map<String, String> requestParams = requester.getRequestParameters();
        List<String> fieldPath = extractFieldPath(stream);

        String methodName = "poll" + ManifestSpec.toClassName(streamName);
        boolean hasPagination = paginator != null && !paginator.hasNoPagination();

        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("final $T streamName = $S", String.class, streamName);

        if (hasPagination) {
            body.add(buildPaginationInit(paginator));
        } else {
            body.addStatement("$T<$T> result = new $T<>()", List.class, SOURCE_RECORD, ArrayList.class);
        }

        body.add(buildUrlBlock(baseUrl, path, requestParams, paginator, hasPagination));
        body.beginControlFlow("try");
        buildRequestStatement(body, auth);
        body.add(buildFetchBlock());
        body.add(buildFieldPathNav(fieldPath, hasPagination));
        body.add(buildNormalizeAndCollect(hasPagination, paginator));
        if (hasPagination) {
            body.add(buildPaginationStateUpdate(paginator));
        }
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

    private CodeBlock buildPaginationInit(PaginatorSpec paginator) {
        CodeBlock.Builder b = CodeBlock.builder();
        b.addStatement("$T<$T> allRecords = new $T<>()", List.class, SOURCE_RECORD, ArrayList.class);
        if (paginator.isCursor()) {
            b.addStatement("$T nextCursor = null", String.class);
            b.beginControlFlow("do");
        } else if (paginator.isPageIncrement()) {
            int start = paginator.getPaginationStrategy() != null
                ? paginator.getPaginationStrategy().getStartFromPage() : 1;
            b.addStatement("int page = $L", start);
            b.beginControlFlow("while (true)");
        } else if (paginator.isOffsetIncrement()) {
            b.addStatement("int offset = 0");
            b.addStatement("final int pageLimit = $L", paginator.pageSize());
            b.beginControlFlow("while (true)");
        }
        return b.build();
    }

    private CodeBlock buildUrlBlock(
        String baseUrl, String path,
        Map<String, String> requestParams,
        PaginatorSpec paginator,
        boolean hasPagination
    ) {
        CodeBlock.Builder b = CodeBlock.builder();
        b.addStatement("$T urlBuilder = new $T($S)", StringBuilder.class, StringBuilder.class,
            baseUrl + path);

        List<String> paramKeys = new ArrayList<>();
        for (Map.Entry<String, String> entry : requestParams.entrySet()) {
            Matcher m = CONFIG_TEMPLATE.matcher(entry.getValue());
            if (m.matches()) {
                paramKeys.add(entry.getKey());
                String getter = "get" + ManifestSpec.toClassName(m.group(1));
                String sep = paramKeys.size() == 1 ? "?" : "&";
                b.addStatement("urlBuilder.append($S + $T.encode(config.$L(), $T.UTF_8))",
                    sep + entry.getKey() + "=",
                    ClassName.get("java.net", "URLEncoder"), getter,
                    ClassName.get("java.nio.charset", "StandardCharsets"));
            }
        }

        if (!hasPagination || paginator == null) return b.build();
        appendPaginationParams(b, paginator, paramKeys.isEmpty());
        return b.build();
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

    private CodeBlock buildFetchBlock() {
        CodeBlock.Builder b = CodeBlock.builder();
        b.addStatement("$T<$T> response = sendWithRetry(request)", HTTP_RESPONSE, String.class);
        b.beginControlFlow("if (response.statusCode() < 200 || response.statusCode() >= 300)");
        b.addStatement(
            "throw new $T(\"HTTP \" + response.statusCode() + \" from \" + urlBuilder)",
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
            b.addStatement(hasPagination ? "return allRecords" : "return $T.emptyList()", Collections.class);
            b.endControlFlow();
            b.addStatement("current = (($T<?, ?>) current).get($S)", Map.class, segment);
            b.beginControlFlow("if (current == null)");
            b.addStatement(hasPagination ? "return allRecords" : "return $T.emptyList()", Collections.class);
            b.endControlFlow();
        }
        b.addStatement("json = current");
        return b.build();
    }

    private CodeBlock buildNormalizeAndCollect(boolean hasPagination, PaginatorSpec paginator) {
        CodeBlock.Builder b = CodeBlock.builder();
        b.addStatement("$T<$T> records", List.class, Object.class);
        b.beginControlFlow("if (json instanceof $T)", List.class);
        b.addStatement("records = ($T<$T>) json", List.class, Object.class);
        b.nextControlFlow("else");
        b.addStatement("records = $T.singletonList(json)", Collections.class);
        b.endControlFlow();

        CodeBlock positionMap = buildPositionMapCode(paginator);
        if (hasPagination) {
            b.beginControlFlow("for ($T record : records)", Object.class);
            b.addStatement("$T value = MAPPER.writeValueAsString(record)", String.class);
            b.addStatement(
                "allRecords.add(new $T($T.of(\"stream\", streamName), $L,"
                    + " streamName, $T.STRING_SCHEMA, value))",
                SOURCE_RECORD, Map.class, positionMap, SCHEMA);
            b.endControlFlow();
        } else {
            b.beginControlFlow("for ($T record : records)", Object.class);
            b.addStatement("$T value = MAPPER.writeValueAsString(record)", String.class);
            b.addStatement(
                "result.add(new $T($T.of(\"stream\", streamName), $L,"
                    + " streamName, $T.STRING_SCHEMA, value))",
                SOURCE_RECORD, Map.class, positionMap, SCHEMA);
            b.endControlFlow();
        }
        return b.build();
    }

    private CodeBlock buildPositionMapCode(PaginatorSpec paginator) {
        if (paginator != null && paginator.isCursor()) {
            return CodeBlock.of("$T.of(\"cursor\", nextCursor != null ? nextCursor : \"\")", Map.class);
        } else if (paginator != null && paginator.isPageIncrement()) {
            return CodeBlock.of("$T.of(\"page\", page)", Map.class);
        } else if (paginator != null && paginator.isOffsetIncrement()) {
            return CodeBlock.of("$T.of(\"offset\", offset)", Map.class);
        }
        return CodeBlock.of("$T.of(\"position\", 0)", Map.class);
    }

    private CodeBlock buildPaginationStateUpdate(PaginatorSpec paginator) {
        CodeBlock.Builder b = CodeBlock.builder();
        if (paginator.isCursor()) {
            b.addStatement("nextCursor = null");
            b.beginControlFlow("if (json instanceof $T)", Map.class);
            b.addStatement("$T<?, ?> respMap = ($T<?, ?>) json", Map.class, Map.class);
            b.addStatement("$T nextToken = respMap.get(\"next_page_token\")", Object.class);
            b.addStatement("nextCursor = nextToken != null ? $T.valueOf(nextToken) : null", String.class);
            b.endControlFlow();
        } else if (paginator.isPageIncrement()) {
            b.beginControlFlow("if (records.isEmpty())");
            b.addStatement("break");
            b.endControlFlow();
            b.addStatement("page++");
        } else if (paginator.isOffsetIncrement()) {
            b.beginControlFlow("if (records.isEmpty())");
            b.addStatement("break");
            b.endControlFlow();
            b.addStatement("offset += pageLimit");
        }
        return b.build();
    }

    private CodeBlock buildPaginationLoopClose(PaginatorSpec paginator) {
        CodeBlock.Builder b = CodeBlock.builder();
        if (paginator.isCursor()) {
            b.endControlFlow("while (nextCursor != null)");
        } else {
            b.endControlFlow();
        }
        b.addStatement("return allRecords");
        return b.build();
    }

    /** Generates the {@code HttpRequest.newBuilder()...build()} statement with auth headers. */
    private void buildRequestStatement(CodeBlock.Builder body, AuthenticatorSpec auth) {
        if (auth == null || auth.isNoAuth()) {
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create(urlBuilder.toString()))\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS
            );
        } else if (auth.isBearer()) {
            String getterName = resolveConfigGetter(auth.getApiToken());
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create(urlBuilder.toString()))\n"
                    + "        .header(\"Authorization\", \"Bearer \" + config.$L())\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, getterName
            );
        } else if (auth.isApiKey()) {
            String headerName = resolveApiKeyHeaderName(auth);
            String getterName = resolveConfigGetter(auth.getApiToken());
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create(urlBuilder.toString()))\n"
                    + "        .header($S, config.$L())\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, headerName, getterName
            );
        } else if (auth.isBasicHttp()) {
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create(urlBuilder.toString()))\n"
                    + "        .header(\"Authorization\", \"Basic \" + cachedCredentials)\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS
            );
        } else if (auth.isOAuth()) {
            body.addStatement("$T accessToken = refreshAccessToken()", String.class);
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create(urlBuilder.toString()))\n"
                    + "        .header(\"Authorization\", \"Bearer \" + accessToken)\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS
            );
        } else {
            body.addStatement(
                "$T request = $T.newBuilder()\n"
                    + "        .uri($T.create(urlBuilder.toString()))\n"
                    + "        .GET()\n"
                    + "        .build()",
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS
            );
        }
    }

    /**
     * Generates a private {@code refreshAccessToken()} method for OAuth connectors.
     * Caches the token using {@code tokenExpiryMs}; refreshes only when expired.
     */
    private MethodSpec buildRefreshAccessToken(ClassName configClass, AuthenticatorSpec auth) {
        String clientIdGetter  = resolveConfigGetter(auth.getClientId());
        String clientSecretGetter = resolveConfigGetter(auth.getClientSecret());
        String refreshTokenGetter = resolveConfigGetter(auth.getRefreshToken());
        String endpoint = auth.getTokenRefreshEndpoint() != null ? auth.getTokenRefreshEndpoint() : "";

        CodeBlock.Builder body = CodeBlock.builder();
        body.beginControlFlow("if (cachedToken != null && $T.currentTimeMillis() < tokenExpiryMs)", System.class);
        body.addStatement("return cachedToken");
        body.endControlFlow();

        body.addStatement("$T reqBody = \"grant_type=refresh_token\"\n"
                + "        + \"&client_id=\" + config.$L()\n"
                + "        + \"&client_secret=\" + config.$L()\n"
                + "        + \"&refresh_token=\" + config.$L()",
            String.class,
            clientIdGetter, clientSecretGetter, refreshTokenGetter
        );
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

    /** Generates a private {@code sendWithRetry()} method that retries on HTTP 429/5xx. */
    private MethodSpec buildSendWithRetry() {
        ParameterizedTypeName httpResponseString = ParameterizedTypeName.get(HTTP_RESPONSE, ClassName.get(String.class));
        CodeBlock.Builder body = CodeBlock.builder();
        body.addStatement("int attempt = 0");
        body.beginControlFlow("while (true)");
        body.addStatement(
            "$T<$T> resp = httpClient.send(request, $T.BodyHandlers.ofString())",
            HTTP_RESPONSE, String.class, HTTP_RESPONSE
        );
        body.beginControlFlow(
            "if ((resp.statusCode() == 429 || resp.statusCode() >= 500) && attempt < 3)");
        body.addStatement("$T.sleep(1000L << attempt)", Thread.class);
        body.addStatement("attempt++");
        body.addStatement("continue");
        body.endControlFlow();
        body.addStatement("return resp");
        body.endControlFlow();

        return MethodSpec.methodBuilder("sendWithRetry")
            .addModifiers(Modifier.PRIVATE)
            .returns(httpResponseString)
            .addParameter(HTTP_REQUEST, "request")
            .addException(Exception.class)
            .addCode(body.build())
            .build();
    }

    private MethodSpec buildStop() {
        return MethodSpec.methodBuilder("stop")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .beginControlFlow("if (httpClient != null)")
            .addStatement("httpClient.close()")
            .endControlFlow()
            .addStatement("httpClient = null")
            .build();
    }

    /**
     * Resolves a config template like {@code {{ config['key'] }}} to the accessor name
     * (e.g., {@code getKey}), or falls back to a safe default when the template doesn't match.
     */
    private String resolveConfigGetter(String template) {
        if (template == null) {
            return "get";
        }
        Matcher m = CONFIG_TEMPLATE.matcher(template.trim());
        if (m.matches()) {
            return "get" + ManifestSpec.toClassName(m.group(1));
        }
        Matcher partial = CONFIG_TEMPLATE.matcher(template);
        if (partial.find()) {
            return "get" + ManifestSpec.toClassName(partial.group(1));
        }
        return "get";
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
}
