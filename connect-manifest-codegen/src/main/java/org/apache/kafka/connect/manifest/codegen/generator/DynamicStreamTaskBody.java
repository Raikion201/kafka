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
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.lang.model.element.Modifier;

/**
 * Builds the {@link TypeSpec} body for a dynamic-stream source task. Class shape is fixed
 * (OAuth refresh, sheet discovery, values:batchGet); per-manifest values flow in through
 * {@link #build}.
 */
final class DynamicStreamTaskBody {

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
    private static final ClassName OBJECT_NODE = ClassName.get(ObjectNode.class);
    private static final ClassName HTTP_CLIENT = ClassName.get(HttpClient.class);
    private static final ClassName HTTP_REQUEST = ClassName.get(HttpRequest.class);
    private static final ClassName HTTP_RESPONSE = ClassName.get(HttpResponse.class);
    private static final ClassName URI_CLASS = ClassName.get(URI.class);
    private static final ClassName URL_ENCODER = ClassName.get(URLEncoder.class);
    private static final ClassName STANDARD_CHARSETS = ClassName.get(StandardCharsets.class);
    private static final ClassName PATTERN = ClassName.get(Pattern.class);

    private static final ParameterizedTypeName MAP_STRING_STRING = ParameterizedTypeName.get(
        ClassName.get("java.util", "Map"), ClassName.get(String.class), ClassName.get(String.class));
    private static final ParameterizedTypeName MAP_STRING_OBJECT = ParameterizedTypeName.get(
        ClassName.get("java.util", "Map"), ClassName.get(String.class), ClassName.get(Object.class));
    private static final ParameterizedTypeName LIST_STRING = ParameterizedTypeName.get(
        ClassName.get("java.util", "List"), ClassName.get(String.class));
    private static final ParameterizedTypeName LIST_RECORD = ParameterizedTypeName.get(
        ClassName.get("java.util", "List"), SOURCE_RECORD);
    private static final ParameterizedTypeName HTTP_RESPONSE_STRING = ParameterizedTypeName.get(
        HTTP_RESPONSE, ClassName.get(String.class));

    private DynamicStreamTaskBody() {
    }

    static TypeSpec build(
        String taskClassName,
        ClassName configClass,
        String tokenEndpoint,
        String selectionKey,
        String selectionPath,
        String discoveryUrl,
        String baseUrlPrefix
    ) {
        TypeSpec.Builder cls = TypeSpec.classBuilder(taskClassName)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .superclass(SOURCE_TASK);
        addFields(cls, configClass);
        cls.addMethod(version());
        cls.addMethod(start(configClass));
        cls.addMethod(poll());
        cls.addMethod(extractSpreadsheetId());
        cls.addMethod(getAccessToken(tokenEndpoint, selectionKey, selectionPath));
        cls.addMethod(selectFromCredentials());
        cls.addMethod(discoverSheets(discoveryUrl));
        cls.addMethod(fetchSheetRows(baseUrlPrefix));
        cls.addMethod(sanitizeTopic());
        cls.addMethod(sendWithRetry());
        cls.addMethod(stop());
        return cls.build();
    }

    private static void addFields(TypeSpec.Builder cls, ClassName configClass) {
        cls.addField(FieldSpec.builder(OBJECT_MAPPER, "MAPPER",
                Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("new $T()", OBJECT_MAPPER).build());
        cls.addField(FieldSpec.builder(PATTERN, "URL_ID_PATTERN",
                Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("$T.compile($S)", PATTERN, "/([-\\w]{20,})").build());
        cls.addField(configClass, "config", Modifier.PRIVATE);
        cls.addField(HTTP_CLIENT, "httpClient", Modifier.PRIVATE);
        cls.addField(MAP_STRING_OBJECT, "credentials", Modifier.PRIVATE);
        cls.addField(LIST_STRING, "sheetNames", Modifier.PRIVATE);
        cls.addField(int.class, "currentSheetIdx", Modifier.PRIVATE);
        cls.addField(boolean.class, "exhausted", Modifier.PRIVATE);
        cls.addField(FieldSpec.builder(String.class, "accessToken",
            Modifier.PRIVATE, Modifier.VOLATILE).build());
        cls.addField(FieldSpec.builder(long.class, "tokenExpiryMs",
            Modifier.PRIVATE, Modifier.VOLATILE).initializer("0L").build());
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
            .addStatement("this.config = new $T(props)", configClass)
            .addStatement("this.httpClient = $T.newHttpClient()", HTTP_CLIENT)
            .addCode(""
                + "String credsJson = config.getCredentials();\n"
                + "if (credsJson == null || credsJson.isBlank()) {\n"
                + "    throw new $T(\"`credentials` config is required (JSON object string)\");\n"
                + "}\n"
                + "try {\n"
                + "    this.credentials = MAPPER.readValue(credsJson, $T.class);\n"
                + "} catch (Exception e) {\n"
                + "    throw new $T(\"`credentials` must be a JSON object\", e);\n"
                + "}\n"
                + "try {\n"
                + "    this.sheetNames = discoverSheets();\n"
                + "} catch (Exception e) {\n"
                + "    throw new $T(\"Failed to discover sheets\", e);\n"
                + "}\n"
                + "this.currentSheetIdx = 0;\n"
                + "this.exhausted = this.sheetNames.isEmpty();\n",
                CONNECT_EXCEPTION, Map.class, CONNECT_EXCEPTION, CONNECT_EXCEPTION)
            .build();
    }

    private static MethodSpec poll() {
        return MethodSpec.methodBuilder("poll")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(LIST_RECORD)
            .addException(InterruptedException.class)
            .addCode(""
                + "if (exhausted || currentSheetIdx >= sheetNames.size()) {\n"
                + "    Thread.sleep(60000L);\n"
                + "    return $T.emptyList();\n"
                + "}\n"
                + "String sheet = sheetNames.get(currentSheetIdx);\n"
                + "currentSheetIdx++;\n"
                + "if (currentSheetIdx >= sheetNames.size()) exhausted = true;\n"
                + "try {\n"
                + "    return fetchSheetRows(sheet);\n"
                + "} catch (Exception e) {\n"
                + "    throw new $T(\"Failed to fetch sheet: \" + sheet, e);\n"
                + "}\n",
                Collections.class, CONNECT_EXCEPTION)
            .build();
    }

    private static MethodSpec extractSpreadsheetId() {
        return MethodSpec.methodBuilder("extractSpreadsheetId")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class)
            .addParameter(String.class, "raw")
            .addCode(""
                + "if (raw == null) return \"\";\n"
                + "if (raw.startsWith(\"http\")) {\n"
                + "    java.util.regex.Matcher m = URL_ID_PATTERN.matcher(raw);\n"
                + "    if (m.find()) return m.group(1);\n"
                + "}\n"
                + "return raw;\n")
            .build();
    }

    private static MethodSpec getAccessToken(String tokenEndpoint, String selectionKey, String selectionPath) {
        return MethodSpec.methodBuilder("getAccessToken")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class)
            .addException(Exception.class)
            .addCode(""
                + "if (accessToken != null && System.currentTimeMillis() < tokenExpiryMs - 30000L) {\n"
                + "    return accessToken;\n"
                + "}\n"
                + "Object selSel = selectFromCredentials($S);\n"
                + "String authType = selSel == null ? $S : String.valueOf(selSel);\n"
                + "if (!$S.equalsIgnoreCase(authType)) {\n"
                + "    throw new $T(\"Only OAuth (auth_type=$L) credentials are supported by codegen; got: \" + authType);\n"
                + "}\n"
                + "String clientId = String.valueOf(credentials.get(\"client_id\"));\n"
                + "String clientSecret = String.valueOf(credentials.get(\"client_secret\"));\n"
                + "String refreshToken = String.valueOf(credentials.get(\"refresh_token\"));\n"
                + "String body = \"grant_type=refresh_token\""
                + " + \"&client_id=\" + $T.encode(clientId, $T.UTF_8)"
                + " + \"&client_secret=\" + $T.encode(clientSecret, $T.UTF_8)"
                + " + \"&refresh_token=\" + $T.encode(refreshToken, $T.UTF_8);\n"
                + "$T req = $T.newBuilder()\n"
                + "    .uri($T.create($S))\n"
                + "    .header(\"Content-Type\", \"application/x-www-form-urlencoded\")\n"
                + "    .POST($T.BodyPublishers.ofString(body))\n"
                + "    .build();\n"
                + "$T<String> resp = httpClient.send(req, $T.BodyHandlers.ofString());\n"
                + "if (resp.statusCode() / 100 != 2) {\n"
                + "    throw new $T(\"OAuth refresh failed: \" + resp.statusCode() + \" \" + resp.body());\n"
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
                selectionPath, selectionKey, selectionKey, CONNECT_EXCEPTION, selectionKey,
                URL_ENCODER, STANDARD_CHARSETS, URL_ENCODER, STANDARD_CHARSETS,
                URL_ENCODER, STANDARD_CHARSETS,
                HTTP_REQUEST, HTTP_REQUEST, URI_CLASS, tokenEndpoint, HTTP_REQUEST,
                HTTP_RESPONSE, HTTP_RESPONSE, CONNECT_EXCEPTION,
                MAP_STRING_OBJECT, Map.class, CONNECT_EXCEPTION)
            .build();
    }

    private static MethodSpec selectFromCredentials() {
        return MethodSpec.methodBuilder("selectFromCredentials")
            .addModifiers(Modifier.PRIVATE)
            .returns(Object.class)
            .addParameter(String.class, "commaPath")
            .addCode(""
                + "if (commaPath == null || commaPath.isEmpty()) return null;\n"
                + "String[] parts = commaPath.split(\",\");\n"
                + "Object cur = credentials;\n"
                + "for (int i = 0; i < parts.length; i++) {\n"
                + "    String key = parts[i];\n"
                + "    if (i == 0 && \"credentials\".equals(key)) continue;\n"
                + "    if (!(cur instanceof $T<?, ?>)) return null;\n"
                + "    cur = (($T<?, ?>) cur).get(key);\n"
                + "    if (cur == null) return null;\n"
                + "}\n"
                + "return cur;\n",
                Map.class, Map.class)
            .build();
    }

    private static MethodSpec discoverSheets(String discoveryUrl) {
        String prefix = JinjaSnippets.stripTemplatedSuffix(discoveryUrl == null ? "" : discoveryUrl);
        return MethodSpec.methodBuilder("discoverSheets")
            .addModifiers(Modifier.PRIVATE)
            .returns(LIST_STRING)
            .addException(Exception.class)
            .addCode(""
                + "String spreadsheetId = extractSpreadsheetId(config.getSpreadsheetId());\n"
                + "if (spreadsheetId.isBlank()) {\n"
                + "    throw new $T(\"`spreadsheet_id` config is required\");\n"
                + "}\n"
                + "$T uri = $T.create($S + spreadsheetId + \"?includeGridData=false&alt=json\");\n"
                + "$T req = $T.newBuilder().uri(uri)\n"
                + "    .header(\"Authorization\", \"Bearer \" + getAccessToken())\n"
                + "    .GET().build();\n"
                + "$T<String> resp = sendWithRetry(req);\n"
                + "if (resp.statusCode() / 100 != 2) {\n"
                + "    throw new $T(\"Failed to fetch spreadsheet metadata: \" + resp.statusCode() + \" \" + resp.body());\n"
                + "}\n"
                + "$T root = MAPPER.readTree(resp.body());\n"
                + "$T<String> names = new $T<>();\n"
                + "for ($T sheet : root.path(\"sheets\")) {\n"
                + "    String t = sheet.path(\"properties\").path(\"title\").asText(\"\");\n"
                + "    if (!t.isEmpty()) names.add(t);\n"
                + "}\n"
                + "return names;\n",
                CONNECT_EXCEPTION, URI_CLASS, URI_CLASS, prefix,
                HTTP_REQUEST, HTTP_REQUEST, HTTP_RESPONSE, CONNECT_EXCEPTION,
                JSON_NODE, List.class, ArrayList.class, JSON_NODE)
            .build();
    }

    private static MethodSpec fetchSheetRows(String baseUrlPrefix) {
        return MethodSpec.methodBuilder("fetchSheetRows")
            .addModifiers(Modifier.PRIVATE)
            .returns(LIST_RECORD)
            .addParameter(String.class, "sheet")
            .addException(Exception.class)
            .addCode(""
                + "String spreadsheetId = extractSpreadsheetId(config.getSpreadsheetId());\n"
                + "long batch;\n"
                + "try { batch = Long.parseLong(config.getBatchSize()); } catch (Exception e) { batch = 1000000L; }\n"
                + "if (batch <= 0L || batch > 1000000L) batch = 1000000L;\n"
                + "String range = $T.encode(sheet + \"!1:\" + batch, $T.UTF_8);\n"
                + "$T uri = $T.create($S + spreadsheetId\n"
                + "    + \"/values:batchGet?ranges=\" + range\n"
                + "    + \"&majorDimension=ROWS&alt=json\");\n"
                + "$T req = $T.newBuilder().uri(uri)\n"
                + "    .header(\"Authorization\", \"Bearer \" + getAccessToken())\n"
                + "    .GET().build();\n"
                + "$T<String> resp = sendWithRetry(req);\n"
                + "if (resp.statusCode() / 100 != 2) {\n"
                + "    throw new $T(\"Failed to fetch sheet \" + sheet + \": \" + resp.statusCode() + \" \" + resp.body());\n"
                + "}\n"
                + "$T root = MAPPER.readTree(resp.body());\n"
                + "$T<$T> records = new $T<>();\n"
                + "String topic = sanitizeTopic(\"google_sheets_\" + sheet);\n"
                + "$T<String, Object> sourcePartition = $T.singletonMap(\"sheet\", sheet);\n"
                + "for ($T vr : root.path(\"valueRanges\")) {\n"
                + "    $T rows = vr.path(\"values\");\n"
                + "    if (!rows.isArray() || rows.size() < 2) continue;\n"
                + "    $T header = rows.get(0);\n"
                + "    for (int i = 1; i < rows.size(); i++) {\n"
                + "        $T rowArr = rows.get(i);\n"
                + "        $T rowObj = MAPPER.createObjectNode();\n"
                + "        for (int c = 0; c < header.size(); c++) {\n"
                + "            String key = header.get(c).asText(\"col_\" + c);\n"
                + "            String val = c < rowArr.size() ? rowArr.get(c).asText(\"\") : \"\";\n"
                + "            rowObj.put(key, val);\n"
                + "        }\n"
                + "        $T<String, Object> sourceOffset = $T.singletonMap(\"row\", (long) i);\n"
                + "        records.add(new $T(sourcePartition, sourceOffset, topic,\n"
                + "            null, $T.STRING_SCHEMA, MAPPER.writeValueAsString(rowObj)));\n"
                + "    }\n"
                + "}\n"
                + "return records;\n",
                URL_ENCODER, STANDARD_CHARSETS, URI_CLASS, URI_CLASS, baseUrlPrefix,
                HTTP_REQUEST, HTTP_REQUEST, HTTP_RESPONSE, CONNECT_EXCEPTION,
                JSON_NODE, List.class, SOURCE_RECORD, ArrayList.class,
                Map.class, Collections.class,
                JSON_NODE, JSON_NODE, JSON_NODE, JSON_NODE, OBJECT_NODE,
                Map.class, Collections.class, SOURCE_RECORD, SCHEMA)
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
