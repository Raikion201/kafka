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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.lang.model.element.Modifier;

/**
 * Generic stub task for dynamic-stream manifests whose shape we don't yet support
 * (anything that isn't google_sheets). The connector still loads cleanly in Connect;
 * the task fails fast on start() with a clear message so operators see "unsupported"
 * rather than silent codegen drift.
 */
final class GenericDynamicStreamStub {

    private static final ClassName SOURCE_TASK =
        ClassName.get("org.apache.kafka.connect.source", "SourceTask");
    private static final ClassName SOURCE_RECORD =
        ClassName.get("org.apache.kafka.connect.source", "SourceRecord");
    private static final ClassName CONNECT_EXCEPTION =
        ClassName.get("org.apache.kafka.connect.errors", "ConnectException");

    private GenericDynamicStreamStub() {
    }

    static TypeSpec build(String taskClassName, ClassName configClass, String connectorClassName) {
        ParameterizedTypeName mapStringString = ParameterizedTypeName.get(
            ClassName.get(Map.class), ClassName.get(String.class), ClassName.get(String.class));
        ParameterizedTypeName listOfRecord = ParameterizedTypeName.get(
            ClassName.get(List.class), SOURCE_RECORD);

        MethodSpec version = MethodSpec.methodBuilder("version")
            .addModifiers(Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return $S", "1.0.0")
            .build();

        MethodSpec start = MethodSpec.methodBuilder("start")
            .addModifiers(Modifier.PUBLIC)
            .addParameter(mapStringString, "props")
            .addStatement(
                "throw new $T($S)",
                CONNECT_EXCEPTION,
                "Dynamic-stream codegen not yet supported for connector '" + connectorClassName
                    + "'. Only google_sheets-shape manifests can be generated."
            )
            .build();

        MethodSpec poll = MethodSpec.methodBuilder("poll")
            .addModifiers(Modifier.PUBLIC)
            .returns(listOfRecord)
            .addException(InterruptedException.class)
            .addStatement("return $T.emptyList()", Collections.class)
            .build();

        MethodSpec stop = MethodSpec.methodBuilder("stop")
            .addModifiers(Modifier.PUBLIC)
            .build();

        return TypeSpec.classBuilder(taskClassName)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(SOURCE_TASK)
            .addMethod(version)
            .addMethod(start)
            .addMethod(poll)
            .addMethod(stop)
            .build();
    }
}
