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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.lang.model.element.Modifier;

/**
 * Builds the {@link TypeSpec} body for an Airtable DynamicDeclarativeStream source task.
 *
 * <p>Generated class discovers all (base, table) pairs at start via the Airtable metadata API
 * and polls each table's records on every {@code poll()} call. Supports both OAuth 2.0 and
 * Personal Access Token authentication.</p>
 */
final class AirtableTaskBody {

    private static final ClassName SOURCE_TASK =
        ClassName.get("org.apache.kafka.connect.source", "SourceTask");
    private static final ClassName SOURCE_RECORD =
        ClassName.get("org.apache.kafka.connect.source", "SourceRecord");
    private static final ClassName SCHEMA =
        ClassName.get("org.apache.kafka.connect.data", "Schema");
    private static final ClassName CONNECT_EXCEPTION =
        ClassName.get("org.apache.kafka.connect.errors", "ConnectException");
    private static final ClassName OBJECT_MAPPER = ClassName.get(ObjectMapper.class);
    private static final ClassName JSON_NODE = ClassName.get(JsonNode.class);
    private static final ClassName HTTP_CLIENT = ClassName.get(HttpClient.class);
    private static final ClassName HTTP_REQUEST = ClassName.get(HttpRequest.class);
    private static final ClassName HTTP_RESPONSE = ClassName.get(HttpResponse.class);
    private static final ClassName URI_CLASS = ClassName.get(URI.class);
    private static final ClassName URL_ENCODER = ClassName.get(URLEncoder.class);
    private static final ClassName STANDARD_CHARSETS = ClassName.get(StandardCharsets.class);
    private static final ClassName BASE64 = ClassName.get(Base64.class);

    private static final ParameterizedTypeName MAP_STRING_STRING = ParameterizedTypeName.get(
        ClassName.get("java.util", "Map"), ClassName.get(String.class), ClassName.get(String.class));
    private static final ParameterizedTypeName MAP_STRING_OBJECT = ParameterizedTypeName.get(
        ClassName.get("java.util", "Map"), ClassName.get(String.class), ClassName.get(Object.class));
    private static final ParameterizedTypeName LIST_SOURCE_RECORD = ParameterizedTypeName.get(
        ClassName.get("java.util", "List"), SOURCE_RECORD);
    private static final ParameterizedTypeName LIST_STRING_ARRAY = ParameterizedTypeName.get(
        ClassName.get("java.util", "List"), ArrayTypeName.of(String.class));
    private static final ParameterizedTypeName HTTP_RESPONSE_STRING = ParameterizedTypeName.get(
        HTTP_RESPONSE, ClassName.get(String.class));

    private AirtableTaskBody() {
    }

    static TypeSpec build(String taskClassName, ClassName configClass) {
        TypeSpec.Builder cls = TypeSpec.classBuilder(taskClassName)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(SOURCE_TASK);
        addFields(cls, configClass);
        cls.addMethod(version());
        cls.addMethod(start(configClass));
        cls.addMethod(poll());
        cls.addMethod(getAccessToken());
        cls.addMethod(discoverStreams());
        cls.addMethod(fetchTablesForBase());
        cls.addMethod(buildStreamName(configClass));
        cls.addMethod(fetchTableRecords());
        cls.addMethod(snakeCase());
        cls.addMethod(sanitizeTopic());
        cls.addMethod(sendWithRetry());
        cls.addMethod(stop());
        return cls.build();
    }

    private static void addFields(TypeSpec.Builder cls, ClassName configClass) {
        cls.addField(FieldSpec.builder(OBJECT_MAPPER, "MAPPER",
                Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("new $T()", OBJECT_MAPPER).build());
        cls.addField(configClass, "config", Modifier.PRIVATE);
        cls.addField(HTTP_CLIENT, "httpClient", Modifier.PRIVATE);
        cls.addField(LIST_STRING_ARRAY, "streams", Modifier.PRIVATE);
        cls.addField(MAP_STRING_OBJECT, "credentials", Modifier.PRIVATE);
        cls.addField(FieldSpec.builder(String.class, "accessToken",
            Modifier.PRIVATE, Modifier.VOLATILE).build());
        cls.addField(FieldSpec.builder(long.class, "tokenExpiryMs",
            Modifier.PRIVATE, Modifier.VOLATILE).initializer("0L").build());
        cls.addField(FieldSpec.builder(boolean.class, "useOAuth",
            Modifier.PRIVATE).build());
    }

    private static MethodSpec version() {
        return MethodSpec.methodBuilder("version")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return $S", "1.0.0")
            .build();
    }

    private static MethodSpec start(ClassName configClass) {
        return MethodSpec.methodBuilder("start")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(MAP_STRING_STRING, "props")
            .addCode(""
                + "this.config = new $T(props);\n"
                + "this.httpClient = $T.newHttpClient();\n"
                + "String credsJson = config.getCredentials();\n"
                + "if (credsJson == null || credsJson.isBlank()) {\n"
                + "    throw new $T(\"`credentials` config is required\");\n"
                + "}\n"
                + "try {\n"
                + "    this.credentials = MAPPER.readValue(credsJson, $T.class);\n"
                + "} catch (Exception e) {\n"
                + "    throw new $T(\"`credentials` must be valid JSON\", e);\n"
                + "}\n"
                + "Object am = credentials.get(\"auth_method\");\n"
                + "this.useOAuth = \"oauth2.0\".equals(String.valueOf(am));\n"
                + "try {\n"
                + "    this.streams = discoverStreams();\n"
                + "} catch (Exception e) {\n"
                + "    throw new $T(\"Failed to discover Airtable streams\", e);\n"
                + "}\n",
                configClass, HTTP_CLIENT, CONNECT_EXCEPTION,
                Map.class, CONNECT_EXCEPTION, CONNECT_EXCEPTION)
            .build();
    }

    private static MethodSpec poll() {
        return MethodSpec.methodBuilder("poll")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(LIST_SOURCE_RECORD)
            .addException(InterruptedException.class)
            .addCode(""
                + "$T<$T> records = new $T<>();\n"
                + "for (String[] s : streams) {\n"
                + "    try {\n"
                + "        records.addAll(fetchTableRecords(s[0], s[1], s[2], s[3]));\n"
                + "    } catch (Exception e) {\n"
                + "        throw new $T(\"Failed to poll Airtable table \" + s[2], e);\n"
                + "    }\n"
                + "}\n"
                + "if (records.isEmpty()) Thread.sleep(5000L);\n"
                + "return records;\n",
                List.class, SOURCE_RECORD, ArrayList.class, CONNECT_EXCEPTION)
            .build();
    }

    private static MethodSpec getAccessToken() {
        return MethodSpec.methodBuilder("getAccessToken")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class)
            .addException(Exception.class)
            .addCode(""
                + "if (!useOAuth) {\n"
                + "    return String.valueOf(credentials.get(\"api_key\"));\n"
                + "}\n"
                + "if (accessToken != null && System.currentTimeMillis() < tokenExpiryMs - 30000L) {\n"
                + "    return accessToken;\n"
                + "}\n"
                + "String clientId = String.valueOf(credentials.get(\"client_id\"));\n"
                + "String clientSecret = String.valueOf(credentials.get(\"client_secret\"));\n"
                + "String refreshToken = String.valueOf(credentials.get(\"refresh_token\"));\n"
                + "String basic = $T.getEncoder().encodeToString(\n"
                + "    (clientId + \":\" + clientSecret).getBytes($T.UTF_8));\n"
                + "String body = \"grant_type=refresh_token&refresh_token=\"\n"
                + "    + $T.encode(refreshToken, $T.UTF_8);\n"
                + "$T req = $T.newBuilder()\n"
                + "    .uri($T.create($S))\n"
                + "    .header(\"Authorization\", \"Basic \" + basic)\n"
                + "    .header(\"Content-Type\", \"application/x-www-form-urlencoded\")\n"
                + "    .POST($T.BodyPublishers.ofString(body))\n"
                + "    .build();\n"
                + "$T<String> resp = httpClient.send(req, $T.BodyHandlers.ofString());\n"
                + "if (resp.statusCode() / 100 != 2) {\n"
                + "    throw new $T(\"Airtable OAuth refresh failed: \" + resp.statusCode()\n"
                + "        + \" \" + resp.body());\n"
                + "}\n"
                + "$T tok = MAPPER.readValue(resp.body(), $T.class);\n"
                + "Object tokenObj = tok.get(\"access_token\");\n"
                + "if (tokenObj == null) {\n"
                + "    throw new $T(\"OAuth response missing access_token: \" + resp.body());\n"
                + "}\n"
                + "this.accessToken = String.valueOf(tokenObj);\n"
                + "Object expiresIn = tok.getOrDefault(\"expires_in\", 3600);\n"
                + "long expSec = (expiresIn instanceof Number n) ? n.longValue() : 3600L;\n"
                + "this.tokenExpiryMs = System.currentTimeMillis() + expSec * 1000L;\n"
                + "return this.accessToken;\n",
                BASE64, STANDARD_CHARSETS,
                URL_ENCODER, STANDARD_CHARSETS,
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, "https://airtable.com/oauth2/v1/token",
                HTTP_REQUEST,
                HTTP_RESPONSE, HTTP_RESPONSE, CONNECT_EXCEPTION,
                MAP_STRING_OBJECT, Map.class, CONNECT_EXCEPTION)
            .build();
    }

    private static MethodSpec discoverStreams() {
        return MethodSpec.methodBuilder("discoverStreams")
            .addModifiers(Modifier.PRIVATE)
            .returns(LIST_STRING_ARRAY)
            .addException(Exception.class)
            .addCode(""
                + "$T<String[]> result = new $T<>();\n"
                + "String offset = null;\n"
                + "do {\n"
                + "    String url = \"https://api.airtable.com/v0/meta/bases\"\n"
                + "        + (offset != null ? \"?offset=\" + $T.encode(offset, $T.UTF_8) : \"\");\n"
                + "    $T req = $T.newBuilder()\n"
                + "        .uri($T.create(url))\n"
                + "        .header(\"Authorization\", \"Bearer \" + getAccessToken())\n"
                + "        .GET().build();\n"
                + "    $T<String> resp = sendWithRetry(req);\n"
                + "    if (resp.statusCode() / 100 != 2) {\n"
                + "        throw new $T(\"Failed to list Airtable bases: \" + resp.statusCode()\n"
                + "            + \" \" + resp.body());\n"
                + "    }\n"
                + "    $T root = MAPPER.readTree(resp.body());\n"
                + "    $T bases = root.path(\"bases\");\n"
                + "    if (bases.isArray()) {\n"
                + "        for ($T base : bases) {\n"
                + "            String baseId = base.path(\"id\").asText(\"\");\n"
                + "            String baseName = base.path(\"name\").asText(baseId);\n"
                + "            result.addAll(fetchTablesForBase(baseId, baseName));\n"
                + "        }\n"
                + "    }\n"
                + "    $T off = root.path(\"offset\");\n"
                + "    offset = off.isMissingNode() || off.isNull() ? null : off.asText(null);\n"
                + "} while (offset != null && !offset.isEmpty());\n"
                + "return result;\n",
                List.class, ArrayList.class,
                URL_ENCODER, STANDARD_CHARSETS,
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS,
                HTTP_RESPONSE, CONNECT_EXCEPTION,
                JSON_NODE, JSON_NODE, JSON_NODE, JSON_NODE)
            .build();
    }

    private static MethodSpec fetchTablesForBase() {
        return MethodSpec.methodBuilder("fetchTablesForBase")
            .addModifiers(Modifier.PRIVATE)
            .returns(LIST_STRING_ARRAY)
            .addParameter(String.class, "baseId")
            .addParameter(String.class, "baseName")
            .addException(Exception.class)
            .addCode(""
                + "$T<String[]> tables = new $T<>();\n"
                + "String offset = null;\n"
                + "do {\n"
                + "    String url = \"https://api.airtable.com/v0/meta/bases/\" + baseId + \"/tables\"\n"
                + "        + (offset != null ? \"?offset=\" + $T.encode(offset, $T.UTF_8) : \"\");\n"
                + "    $T req = $T.newBuilder()\n"
                + "        .uri($T.create(url))\n"
                + "        .header(\"Authorization\", \"Bearer \" + getAccessToken())\n"
                + "        .GET().build();\n"
                + "    $T<String> resp = sendWithRetry(req);\n"
                + "    if (resp.statusCode() / 100 != 2) continue;\n"
                + "    $T root = MAPPER.readTree(resp.body());\n"
                + "    $T tArr = root.path(\"tables\");\n"
                + "    if (tArr.isArray()) {\n"
                + "        for ($T t : tArr) {\n"
                + "            String tableId = t.path(\"id\").asText(\"\");\n"
                + "            String tableName = t.path(\"name\").asText(tableId);\n"
                + "            String streamName = buildStreamName(baseName, baseId, tableName, tableId);\n"
                + "            tables.add(new String[]{baseId, tableId, streamName, tableName});\n"
                + "        }\n"
                + "    }\n"
                + "    $T off = root.path(\"offset\");\n"
                + "    offset = off.isMissingNode() || off.isNull() ? null : off.asText(null);\n"
                + "} while (offset != null && !offset.isEmpty());\n"
                + "return tables;\n",
                List.class, ArrayList.class,
                URL_ENCODER, STANDARD_CHARSETS,
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS,
                HTTP_RESPONSE,
                JSON_NODE, JSON_NODE, JSON_NODE, JSON_NODE)
            .build();
    }

    private static MethodSpec buildStreamName(ClassName configClass) {
        return MethodSpec.methodBuilder("buildStreamName")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class)
            .addParameter(String.class, "baseName")
            .addParameter(String.class, "baseId")
            .addParameter(String.class, "tableName")
            .addParameter(String.class, "tableId")
            .addCode(""
                + "String bSnake = snakeCase(baseName);\n"
                + "String tSnake = snakeCase(tableName);\n"
                + "if (config.getAddBaseIdToStreamName()) {\n"
                + "    return bSnake + \"/\" + baseId + \"/\" + tSnake + \"/\" + tableId;\n"
                + "}\n"
                + "return bSnake + \"/\" + tSnake + \"/\" + tableId;\n")
            .build();
    }

    private static MethodSpec fetchTableRecords() {
        return MethodSpec.methodBuilder("fetchTableRecords")
            .addModifiers(Modifier.PRIVATE)
            .returns(LIST_SOURCE_RECORD)
            .addParameter(String.class, "baseId")
            .addParameter(String.class, "tableId")
            .addParameter(String.class, "streamName")
            .addParameter(String.class, "tableName")
            .addException(Exception.class)
            .addCode(""
                + "$T<$T> records = new $T<>();\n"
                + "String topic = sanitizeTopic(\"airtable_\" + streamName);\n"
                + "$T<String, Object> srcPart = $T.singletonMap(\"stream\", streamName);\n"
                + "String cursor = null;\n"
                + "do {\n"
                + "    String url = \"https://api.airtable.com/v0/\" + baseId + \"/\" + tableId\n"
                + "        + \"?pageSize=100\"\n"
                + "        + (cursor != null ? \"&offset=\" + $T.encode(cursor, $T.UTF_8) : \"\");\n"
                + "    $T req = $T.newBuilder()\n"
                + "        .uri($T.create(url))\n"
                + "        .header(\"Authorization\", \"Bearer \" + getAccessToken())\n"
                + "        .GET().build();\n"
                + "    $T<String> resp = sendWithRetry(req);\n"
                + "    if (resp.statusCode() / 100 != 2) {\n"
                + "        throw new $T(\"Failed to fetch \" + streamName + \": \"\n"
                + "            + resp.statusCode() + \" \" + resp.body());\n"
                + "    }\n"
                + "    $T root = MAPPER.readTree(resp.body());\n"
                + "    $T recArr = root.path(\"records\");\n"
                + "    if (recArr.isArray()) {\n"
                + "        for ($T rec : recArr) {\n"
                + "            $T<String, Object> row = new $T<>();\n"
                + "            $T fields = rec.path(\"fields\");\n"
                + "            if (fields.isObject()) {\n"
                + "                fields.properties().forEach(e ->\n"
                + "                    row.put(e.getKey(), e.getValue().asText(\"\")));\n"
                + "            }\n"
                + "            row.put(\"_airtable_id\", rec.path(\"id\").asText(\"\"));\n"
                + "            row.put(\"_airtable_created_time\", rec.path(\"createdTime\").asText(\"\"));\n"
                + "            row.put(\"_airtable_table_name\", tableName);\n"
                + "            String recId = rec.path(\"id\").asText(String.valueOf(records.size()));\n"
                + "            $T<String, Object> srcOff = $T.singletonMap(\"id\", recId);\n"
                + "            records.add(new $T(srcPart, srcOff, topic,\n"
                + "                $T.STRING_SCHEMA, recId,\n"
                + "                $T.STRING_SCHEMA, MAPPER.writeValueAsString(row)));\n"
                + "        }\n"
                + "    }\n"
                + "    $T off = root.path(\"offset\");\n"
                + "    cursor = off.isMissingNode() || off.isNull() ? null : off.asText(null);\n"
                + "} while (cursor != null && !cursor.isEmpty());\n"
                + "return records;\n",
                List.class, SOURCE_RECORD, ArrayList.class,
                Map.class, Collections.class,
                URL_ENCODER, STANDARD_CHARSETS,
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS,
                HTTP_RESPONSE, CONNECT_EXCEPTION,
                JSON_NODE, JSON_NODE, JSON_NODE,
                Map.class, LinkedHashMap.class,
                JSON_NODE,
                Map.class, Collections.class,
                SOURCE_RECORD, SCHEMA, SCHEMA,
                JSON_NODE)
            .build();
    }

    private static MethodSpec snakeCase() {
        return MethodSpec.methodBuilder("snakeCase")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class)
            .addParameter(String.class, "s")
            .addStatement("return s == null ? $S : s.replace(' ', '_').toLowerCase($T.ROOT).strip()",
                "", java.util.Locale.class)
            .build();
    }

    private static MethodSpec sanitizeTopic() {
        return MethodSpec.methodBuilder("sanitizeTopic")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class)
            .addParameter(String.class, "s")
            .addStatement("return s.replaceAll($S, $S)", "[^a-zA-Z0-9._-]", "_")
            .build();
    }

    private static MethodSpec sendWithRetry() {
        return MethodSpec.methodBuilder("sendWithRetry")
            .addModifiers(Modifier.PRIVATE)
            .returns(HTTP_RESPONSE_STRING)
            .addParameter(HTTP_REQUEST, "request")
            .addException(Exception.class)
            .addCode(""
                + "int attempt = 0;\n"
                + "while (true) {\n"
                + "    $T<String> resp = httpClient.send(request, $T.BodyHandlers.ofString());\n"
                + "    if ((resp.statusCode() == 429 || resp.statusCode() >= 500) && attempt < 3) {\n"
                + "        Thread.sleep(1000L << attempt);\n"
                + "        attempt++;\n"
                + "        continue;\n"
                + "    }\n"
                + "    return resp;\n"
                + "}\n",
                HTTP_RESPONSE, HTTP_RESPONSE)
            .build();
    }

    private static MethodSpec stop() {
        return MethodSpec.methodBuilder("stop")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addCode(""
                + "if (httpClient instanceof AutoCloseable ac) {\n"
                + "    try { ac.close(); } catch (Exception ignored) { }\n"
                + "}\n"
                + "httpClient = null;\n")
            .build();
    }
}
