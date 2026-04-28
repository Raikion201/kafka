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

import org.apache.kafka.connect.manifest.codegen.model.ManifestSpec;
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

import javax.lang.model.element.Modifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generates {@code XxxSourceTask.java} from an Airbyte manifest stream definition.
 *
 * <p>The generated task:
 * <ul>
 *   <li>Extends {@code SourceTask}.</li>
 *   <li>Uses {@code java.net.http.HttpClient} to call the configured endpoint.</li>
 *   <li>Navigates the {@code field_path} in the JSON response to extract records.</li>
 *   <li>Injects {@code request_parameters} from config (supports {@code {{ config['key'] }}} templates).</li>
 *   <li>Publishes records as JSON strings to a topic named after the stream.</li>
 * </ul>
 */
public class TaskGenerator {

    /** Matches Airbyte config-interpolation templates like {@code {{ config['key'] }}}. */
    private static final Pattern CONFIG_TEMPLATE = Pattern.compile(
        "\\{\\{\\s*config\\['([^']+)'\\]\\s*\\}\\}");

    private static final ClassName SOURCE_TASK =
        ClassName.get("org.apache.kafka.connect.source", "SourceTask");
    private static final ClassName SOURCE_RECORD =
        ClassName.get("org.apache.kafka.connect.source", "SourceRecord");
    private static final ClassName SCHEMA =
        ClassName.get("org.apache.kafka.connect.data", "Schema");
    private static final ClassName CONNECT_EXCEPTION =
        ClassName.get("org.apache.kafka.connect.errors", "ConnectException");

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
        StreamSpec stream = streams.get(0);
        if (stream.getRetriever() == null) {
            throw new CodegenException("Stream '" + stream.getName() + "' has no retriever");
        }
        RequesterSpec requester = stream.getRetriever().getRequester();
        if (requester == null) {
            throw new CodegenException("Stream '" + stream.getName() + "' retriever has no requester");
        }

        String baseName = ManifestSpec.toClassName(
            spec.connectorClassName().replace("Source", ""));
        String taskClassName = baseName + "SourceTask";
        String configClassName = baseName + "ConnectorConfig";
        ClassName configClass = ClassName.get(pkgName, configClassName);

        List<String> fieldPath = extractFieldPath(stream);
        Map<String, String> requestParams = requester.getRequestParameters();
        String baseUrl = requester.effectiveBaseUrl();
        String path = requester.getPath();
        String streamName = stream.getName();

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
            .superclass(SOURCE_TASK)
            .addField(
                FieldSpec.builder(String.class, "STREAM_NAME", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                    .initializer("$S", streamName)
                    .build()
            )
            .addField(
                FieldSpec.builder(String.class, "BASE_URL", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                    .initializer("$S", baseUrl)
                    .build()
            )
            .addField(
                FieldSpec.builder(String.class, "PATH", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                    .initializer("$S", path)
                    .build()
            )
            .addField(configClass, "config", Modifier.PRIVATE)
            .addField(
                ClassName.get("java.net.http", "HttpClient"),
                "httpClient",
                Modifier.PRIVATE
            );

        typeBuilder.addMethod(
            MethodSpec.methodBuilder("version")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addStatement("return $S", APP_VERSION)
                .build()
        );

        typeBuilder.addMethod(buildStart(mapStringString, configClass));
        typeBuilder.addMethod(buildPoll(listOfSourceRecord, fieldPath, requestParams, configClass));
        typeBuilder.addMethod(
            MethodSpec.methodBuilder("stop")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .addComment("no persistent resources to release")
                .build()
        );

        return JavaFile.builder(pkgName, typeBuilder.build())
            .skipJavaLangImports(true)
            .build();
    }

    private MethodSpec buildStart(
        ParameterizedTypeName mapStringString,
        ClassName configClass
    ) {
        return MethodSpec.methodBuilder("start")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(mapStringString, "props")
            .addStatement("this.config = new $T(props)", configClass)
            .addStatement("this.httpClient = $T.newHttpClient()",
                ClassName.get("java.net.http", "HttpClient"))
            .build();
    }

    private MethodSpec buildPoll(
        ParameterizedTypeName listOfSourceRecord,
        List<String> fieldPath,
        Map<String, String> requestParams,
        ClassName configClass
    ) throws CodegenException {
        CodeBlock.Builder body = CodeBlock.builder();

        // Build URL
        body.addStatement("$T urlBuilder = new $T($T.BASE_URL + $T.PATH)",
            StringBuilder.class, StringBuilder.class,
            ClassName.get("", ""), ClassName.get("", ""));

        // Fix reference — use the constants directly
        body = CodeBlock.builder();
        body.addStatement("$T urlBuilder = new $T(BASE_URL + PATH)",
            StringBuilder.class, StringBuilder.class);

        // Request parameters
        List<String> paramKeys = new ArrayList<>();
        for (Map.Entry<String, String> entry : requestParams.entrySet()) {
            String paramKey = entry.getKey();
            String paramValue = entry.getValue();
            Matcher m = CONFIG_TEMPLATE.matcher(paramValue);
            if (m.matches()) {
                paramKeys.add(paramKey);
                String configKey = m.group(1);
                String getterName = "get" + ManifestSpec.toClassName(configKey);
                if (paramKeys.size() == 1) {
                    body.addStatement("urlBuilder.append($S + $T.encode(config.$L(), $T.UTF_8))",
                        "?" + paramKey + "=",
                        ClassName.get("java.net", "URLEncoder"),
                        getterName,
                        ClassName.get("java.nio.charset", "StandardCharsets"));
                } else {
                    body.addStatement("urlBuilder.append($S + $T.encode(config.$L(), $T.UTF_8))",
                        "&" + paramKey + "=",
                        ClassName.get("java.net", "URLEncoder"),
                        getterName,
                        ClassName.get("java.nio.charset", "StandardCharsets"));
                }
            }
        }

        // Build and send request
        body.beginControlFlow("try");
        body.addStatement(
            "$T request = $T.newBuilder()\n"
                + "    .uri($T.create(urlBuilder.toString()))\n"
                + "    .GET()\n"
                + "    .build()",
            ClassName.get("java.net.http", "HttpRequest"),
            ClassName.get("java.net.http", "HttpRequest"),
            ClassName.get("java.net", "URI")
        );
        body.addStatement(
            "$T<$T> response = httpClient.send(request, $T.BodyHandlers.ofString())",
            ClassName.get("java.net.http", "HttpResponse"),
            String.class,
            ClassName.get("java.net.http", "HttpResponse")
        );
        body.beginControlFlow("if (response.statusCode() < 200 || response.statusCode() >= 300)");
        body.addStatement(
            "throw new $T(\"HTTP \" + response.statusCode() + \" from \" + urlBuilder)",
            CONNECT_EXCEPTION
        );
        body.endControlFlow();

        // Parse JSON
        body.addStatement(
            "$T mapper = new $T()",
            ClassName.get("com.fasterxml.jackson.databind", "ObjectMapper"),
            ClassName.get("com.fasterxml.jackson.databind", "ObjectMapper")
        );
        body.addStatement(
            "$T json = mapper.readValue(response.body(), $T.class)",
            Object.class,
            Object.class
        );

        // Navigate field_path
        if (!fieldPath.isEmpty()) {
            body.addStatement("$T current = json", Object.class);
            for (String segment : fieldPath) {
                body.beginControlFlow("if (!(current instanceof $T))", Map.class);
                body.addStatement("return $T.emptyList()", Collections.class);
                body.endControlFlow();
                body.addStatement(
                    "current = (($T<?, ?>) current).get($S)",
                    Map.class, segment
                );
                body.beginControlFlow("if (current == null)");
                body.addStatement("return $T.emptyList()", Collections.class);
                body.endControlFlow();
            }
            body.addStatement("json = current");
        }

        // Normalise to list
        body.addStatement("$T<$T> records", List.class, Object.class);
        body.beginControlFlow("if (json instanceof $T)", List.class);
        body.addStatement("records = ($T<$T>) json", List.class, Object.class);
        body.nextControlFlow("else");
        body.addStatement("records = $T.singletonList(json)", Collections.class);
        body.endControlFlow();

        // Convert to SourceRecord
        body.addStatement(
            "$T<$T> result = new $T<>()",
            List.class, SOURCE_RECORD, ArrayList.class
        );
        body.beginControlFlow("for ($T record : records)", Object.class);
        body.addStatement("$T value = mapper.writeValueAsString(record)", String.class);
        body.addStatement(
            "result.add(new $T($T.of(\"stream\", STREAM_NAME), $T.of(\"position\", 0),"
                + " STREAM_NAME, $T.STRING_SCHEMA, value))",
            SOURCE_RECORD,
            Map.class,
            Map.class,
            SCHEMA
        );
        body.endControlFlow();
        body.addStatement("return result");

        body.nextControlFlow("catch ($T e)", InterruptedException.class);
        body.addStatement("$T.currentThread().interrupt()", Thread.class);
        body.addStatement("throw new $T(\"Interrupted while polling " + "\" + STREAM_NAME, e)", CONNECT_EXCEPTION);
        body.nextControlFlow("catch ($T e)", Exception.class);
        body.addStatement("throw new $T(\"Failed to poll \" + STREAM_NAME, e)", CONNECT_EXCEPTION);
        body.endControlFlow();

        return MethodSpec.methodBuilder("poll")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(listOfSourceRecord)
            .addException(InterruptedException.class)
            .addCode(body.build())
            .build();
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
