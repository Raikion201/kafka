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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;

import javax.lang.model.element.Modifier;

import java.util.Collections;
import java.util.List;

/**
 * Generates {@code XxxSourceConnector.java} from an Airbyte manifest.
 *
 * <p>The generated connector:
 * <ul>
 *   <li>Extends {@code SourceConnector}.</li>
 *   <li>Holds and validates configuration via the generated {@code XxxConnectorConfig}.</li>
 *   <li>Returns {@code XxxSourceTask.class} from {@link #taskClass()}.</li>
 *   <li>Propagates the raw props map to each task via {@link #taskConfigs(int)}.</li>
 * </ul>
 */
public class ConnectorGenerator {

    private static final ClassName SOURCE_CONNECTOR =
        ClassName.get("org.apache.kafka.connect.source", "SourceConnector");
    private static final ClassName TASK =
        ClassName.get("org.apache.kafka.connect.connector", "Task");
    private static final ClassName CONFIG_DEF =
        ClassName.get("org.apache.kafka.common.config", "ConfigDef");
    private static final String APP_VERSION = "1.0.0";

    /**
     * Generate the connector class source file.
     *
     * @param spec    the parsed manifest
     * @param pkgName target Java package
     * @return a {@link JavaFile} ready to be written to disk
     * @throws CodegenException if the spec is malformed
     */
    public JavaFile generate(ManifestSpec spec, String pkgName) throws CodegenException {
        String baseName = ManifestSpec.toClassName(
            spec.connectorClassName().replace("Source", ""));
        String connectorClassName = baseName + "SourceConnector";
        String configClassName = baseName + "ConnectorConfig";
        String taskClassName = baseName + "SourceTask";

        ClassName configClass = ClassName.get(pkgName, configClassName);
        ClassName taskClass = ClassName.get(pkgName, taskClassName);

        ParameterizedTypeName mapStringString = ParameterizedTypeName.get(
            ClassName.get("java.util", "Map"),
            ClassName.get(String.class),
            ClassName.get(String.class)
        );
        ParameterizedTypeName listOfMapStringString = ParameterizedTypeName.get(
            ClassName.get("java.util", "List"),
            mapStringString
        );

        TypeSpec.Builder typeBuilder = TypeSpec.classBuilder(connectorClassName)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(SOURCE_CONNECTOR)
            .addField(mapStringString, "props", Modifier.PRIVATE)
            .addField(configClass, "config", Modifier.PRIVATE);

        // version()
        typeBuilder.addMethod(
            MethodSpec.methodBuilder("version")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addStatement("return $S", APP_VERSION)
                .build()
        );

        // start(Map<String, String> props)
        typeBuilder.addMethod(
            MethodSpec.methodBuilder("start")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .addParameter(mapStringString, "props")
                .addStatement("this.props = props")
                .addStatement("this.config = new $T(props)", configClass)
                .build()
        );

        // taskClass()
        typeBuilder.addMethod(
            MethodSpec.methodBuilder("taskClass")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(
                    ClassName.get("java.lang", "Class"),
                    WildcardTypeName.subtypeOf(TASK)
                ))
                .addStatement("return $T.class", taskClass)
                .build()
        );

        // taskConfigs(int maxTasks)
        typeBuilder.addMethod(
            MethodSpec.methodBuilder("taskConfigs")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(listOfMapStringString)
                .addParameter(int.class, "maxTasks")
                .addStatement("return $T.singletonList(props)", Collections.class)
                .build()
        );

        // stop()
        typeBuilder.addMethod(
            MethodSpec.methodBuilder("stop")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .addComment("no persistent resources to release")
                .build()
        );

        // config()
        typeBuilder.addMethod(
            MethodSpec.methodBuilder("config")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(CONFIG_DEF)
                .addStatement("return $T.config()", configClass)
                .build()
        );

        return JavaFile.builder(pkgName, typeBuilder.build())
            .addStaticImport(List.class, "of")
            .skipJavaLangImports(true)
            .build();
    }
}
