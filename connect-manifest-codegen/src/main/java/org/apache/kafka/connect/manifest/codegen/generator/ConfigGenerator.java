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
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;

import javax.lang.model.element.Modifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Generates {@code XxxConnectorConfig.java} from the {@code spec.connection_specification}
 * section of an Airbyte manifest.
 *
 * <p>Each property in the spec becomes:
 * <ul>
 *   <li>A {@code public static final String} key constant.</li>
 *   <li>A {@code ConfigDef} entry added in a static {@code config()} factory method.</li>
 *   <li>A typed accessor method.</li>
 * </ul>
 */
public class ConfigGenerator {

    public static final String BASE_PACKAGE = "org.apache.kafka.connect.manifest.generated";

    private static final ClassName CONFIG_DEF =
        ClassName.get("org.apache.kafka.common.config", "ConfigDef");
    private static final ClassName ABSTRACT_CONFIG =
        ClassName.get("org.apache.kafka.common.config", "AbstractConfig");

    /**
     * Generate the config class source file.
     *
     * @param spec    the parsed manifest
     * @param pkgName target Java package
     * @return a {@link JavaFile} ready to be written to disk
     * @throws CodegenException if the spec is malformed
     */
    public JavaFile generate(ManifestSpec spec, String pkgName) throws CodegenException {
        String className = ManifestSpec.toClassName(spec.connectorClassName().replace("Source", "")) + "ConnectorConfig";
        List<ConfigField> fields = extractFields(spec);

        TypeSpec.Builder typeBuilder = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(ABSTRACT_CONFIG);

        // Key constants
        for (ConfigField f : fields) {
            typeBuilder.addField(
                FieldSpec.builder(String.class, f.constantName(), Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                    .initializer("$S", f.key())
                    .build()
            );
        }

        // static config() factory
        typeBuilder.addMethod(buildConfigFactory(fields));

        // constructor
        typeBuilder.addMethod(
            MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(
                    ParameterizedTypeName.get(
                        ClassName.get("java.util", "Map"),
                        WildcardTypeName.subtypeOf(String.class),
                        WildcardTypeName.subtypeOf(Object.class)
                    ),
                    "originals"
                )
                .addStatement("super(config(), originals)")
                .build()
        );

        // accessors
        for (ConfigField f : fields) {
            typeBuilder.addMethod(buildAccessor(f));
        }

        return JavaFile.builder(pkgName, typeBuilder.build())
            .skipJavaLangImports(true)
            .build();
    }

    private MethodSpec buildConfigFactory(List<ConfigField> fields) {
        CodeBlock.Builder body = CodeBlock.builder()
            .addStatement("$T def = new $T()", CONFIG_DEF, CONFIG_DEF);

        for (ConfigField f : fields) {
            body.addStatement(
                "def.define($L, $T.Type.$L, $T.Importance.$L, $S)",
                f.constantName(),
                CONFIG_DEF,
                f.configDefType(),
                CONFIG_DEF,
                f.importance(),
                f.doc()
            );
        }

        body.addStatement("return def");

        return MethodSpec.methodBuilder("config")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .returns(CONFIG_DEF)
            .addCode(body.build())
            .build();
    }

    private MethodSpec buildAccessor(ConfigField f) {
        String getterName = "get" + ManifestSpec.toClassName(f.key());
        return MethodSpec.methodBuilder(getterName)
            .addModifiers(Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return getString($L)", f.constantName())
            .build();
    }

    @SuppressWarnings("unchecked")
    private List<ConfigField> extractFields(ManifestSpec spec) throws CodegenException {
        if (spec.getSpec() == null) {
            return Collections.emptyList();
        }
        Map<String, Object> connSpec = spec.getSpec().getConnectionSpecification();
        Object propsObj = connSpec.get("properties");
        if (propsObj == null) {
            return Collections.emptyList();
        }
        if (!(propsObj instanceof Map)) {
            throw new CodegenException("spec.connection_specification.properties must be a map, got: " + propsObj.getClass());
        }
        Map<String, Object> props = (Map<String, Object>) propsObj;
        List<String> required = extractRequired(connSpec);

        List<ConfigField> result = new ArrayList<>();
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            String key = entry.getKey();
            String doc = extractDoc(entry.getValue());
            boolean isRequired = required.contains(key);
            result.add(new ConfigField(key, doc, isRequired));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<String> extractRequired(Map<String, Object> connSpec) {
        Object req = connSpec.get("required");
        if (req instanceof List) {
            List<?> list = (List<?>) req;
            List<String> result = new ArrayList<>();
            for (Object o : list) {
                if (o instanceof String) {
                    result.add((String) o);
                }
            }
            return result;
        }
        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    private String extractDoc(Object propDef) {
        if (propDef instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) propDef;
            Object desc = m.get("description");
            if (desc != null) {
                return desc.toString();
            }
            Object title = m.get("title");
            if (title != null) {
                return title.toString();
            }
        }
        return "";
    }

    /** Internal value object representing one config property. */
    static final class ConfigField {

        private final String key;
        private final String doc;
        private final boolean required;

        ConfigField(String key, String doc, boolean required) {
            this.key = key;
            this.doc = doc;
            this.required = required;
        }

        String key() {
            return key;
        }

        String doc() {
            return doc;
        }

        boolean isRequired() {
            return required;
        }

        String constantName() {
            return key.toUpperCase(java.util.Locale.ROOT).replace('-', '_') + "_CONFIG";
        }

        String configDefType() {
            return "STRING";
        }

        String importance() {
            return required ? "HIGH" : "MEDIUM";
        }
    }
}
