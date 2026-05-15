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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
 * Builds the {@link TypeSpec} body for a Google Analytics Data API
 * {@code DynamicDeclarativeStream} source task.
 *
 * <p>Generated class uses {@code ConfigComponentsResolver} shape: 57 default
 * report definitions are embedded; users may override via
 * {@code custom_reports_array}. Supports both Client OAuth2 and Service-Account
 * JWT (RS256) authentication per the manifest's SelectiveAuthenticator.</p>
 */
final class GoogleAnalyticsDataApiTaskBody {

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
    private static final ClassName ARRAY_NODE = ClassName.get(ArrayNode.class);
    private static final ClassName OBJECT_NODE = ClassName.get(ObjectNode.class);
    private static final ClassName HTTP_CLIENT = ClassName.get(HttpClient.class);
    private static final ClassName HTTP_REQUEST = ClassName.get(HttpRequest.class);
    private static final ClassName HTTP_RESPONSE = ClassName.get(HttpResponse.class);
    private static final ClassName URI_CLASS = ClassName.get(URI.class);
    private static final ClassName URL_ENCODER = ClassName.get(URLEncoder.class);
    private static final ClassName STANDARD_CHARSETS = ClassName.get(StandardCharsets.class);
    private static final ClassName BASE64 = ClassName.get(Base64.class);
    private static final ClassName LOCAL_DATE = ClassName.get("java.time", "LocalDate");
    private static final ClassName KEY_FACTORY = ClassName.get("java.security", "KeyFactory");
    private static final ClassName PRIVATE_KEY = ClassName.get("java.security", "PrivateKey");
    private static final ClassName JDK_SIGNATURE = ClassName.get("java.security", "Signature");
    private static final ClassName PKCS8_SPEC =
        ClassName.get("java.security.spec", "PKCS8EncodedKeySpec");

    private static final ParameterizedTypeName MAP_STRING_STRING = ParameterizedTypeName.get(
        ClassName.get("java.util", "Map"),
        ClassName.get(String.class), ClassName.get(String.class));
    private static final ParameterizedTypeName MAP_STRING_OBJECT = ParameterizedTypeName.get(
        ClassName.get("java.util", "Map"),
        ClassName.get(String.class), ClassName.get(Object.class));
    private static final ParameterizedTypeName LIST_STRING = ParameterizedTypeName.get(
        ClassName.get("java.util", "List"), ClassName.get(String.class));
    private static final ParameterizedTypeName LIST_SOURCE_RECORD = ParameterizedTypeName.get(
        ClassName.get("java.util", "List"), SOURCE_RECORD);
    private static final ParameterizedTypeName LIST_OBJECT_ARRAY = ParameterizedTypeName.get(
        ClassName.get("java.util", "List"), ArrayTypeName.of(Object.class));
    private static final ParameterizedTypeName HTTP_RESPONSE_STRING = ParameterizedTypeName.get(
        HTTP_RESPONSE, ClassName.get(String.class));

    /** Report definitions: {name, String[] dimensions, String[] metrics}. */
    private static final Object[][] REPORTS = {
        {"daily_active_users",
            new String[]{"date"},
            new String[]{"active1DayUsers"}},
        {"weekly_active_users",
            new String[]{"date"},
            new String[]{"active7DayUsers"}},
        {"four_weekly_active_users",
            new String[]{"date"},
            new String[]{"active28DayUsers"}},
        {"devices",
            new String[]{"date", "deviceCategory", "operatingSystem", "browser"},
            new String[]{"totalUsers", "newUsers", "sessions", "sessionsPerUser",
                "averageSessionDuration", "screenPageViews", "screenPageViewsPerSession",
                "bounceRate"}},
        {"locations",
            new String[]{"region", "country", "city", "date"},
            new String[]{"totalUsers", "newUsers", "sessions", "sessionsPerUser",
                "averageSessionDuration", "screenPageViews", "screenPageViewsPerSession",
                "bounceRate"}},
        {"pages",
            new String[]{"date", "hostName", "pagePathPlusQueryString"},
            new String[]{"screenPageViews", "bounceRate"}},
        {"traffic_sources",
            new String[]{"date", "sessionSource", "sessionMedium"},
            new String[]{"totalUsers", "newUsers", "sessions", "sessionsPerUser",
                "averageSessionDuration", "screenPageViews", "screenPageViewsPerSession",
                "bounceRate"}},
        {"website_overview",
            new String[]{"date"},
            new String[]{"totalUsers", "newUsers", "sessions", "sessionsPerUser",
                "averageSessionDuration", "screenPageViews", "screenPageViewsPerSession",
                "bounceRate"}},
        {"user_acquisition_first_user_medium_report",
            new String[]{"date", "firstUserMedium"},
            new String[]{"newUsers", "engagedSessions", "engagementRate", "eventCount",
                "conversions", "totalRevenue", "totalUsers", "userEngagementDuration"}},
        {"user_acquisition_first_user_source_report",
            new String[]{"date", "firstUserSource"},
            new String[]{"newUsers", "engagedSessions", "engagementRate", "eventCount",
                "conversions", "totalRevenue", "totalUsers", "userEngagementDuration"}},
        {"user_acquisition_first_user_source_medium_report",
            new String[]{"date", "firstUserSource", "firstUserMedium"},
            new String[]{"newUsers", "engagedSessions", "engagementRate", "eventCount",
                "conversions", "totalRevenue", "totalUsers", "userEngagementDuration"}},
        {"user_acquisition_first_user_source_platform_report",
            new String[]{"date", "firstUserSourcePlatform"},
            new String[]{"newUsers", "engagedSessions", "engagementRate", "eventCount",
                "conversions", "totalRevenue", "totalUsers", "userEngagementDuration"}},
        {"user_acquisition_first_user_campaign_report",
            new String[]{"date", "firstUserCampaignName"},
            new String[]{"newUsers", "engagedSessions", "engagementRate", "eventCount",
                "conversions", "totalRevenue", "totalUsers", "userEngagementDuration"}},
        {"user_acquisition_first_user_google_ads_ad_network_type_report",
            new String[]{"date", "firstUserGoogleAdsAdNetworkType"},
            new String[]{"newUsers", "engagedSessions", "engagementRate", "eventCount",
                "conversions", "totalRevenue", "totalUsers", "userEngagementDuration"}},
        {"user_acquisition_first_user_google_ads_ad_group_name_report",
            new String[]{"date", "firstUserGoogleAdsAdGroupName"},
            new String[]{"newUsers", "engagedSessions", "engagementRate", "eventCount",
                "conversions", "totalRevenue", "totalUsers", "userEngagementDuration"}},
        {"traffic_acquisition_session_source_medium_report",
            new String[]{"date", "sessionSource", "sessionMedium"},
            new String[]{"totalUsers", "sessions", "engagedSessions", "eventsPerSession",
                "engagementRate", "eventCount", "conversions", "totalRevenue",
                "userEngagementDuration"}},
        {"traffic_acquisition_session_medium_report",
            new String[]{"date", "sessionMedium"},
            new String[]{"totalUsers", "sessions", "engagedSessions", "eventsPerSession",
                "engagementRate", "eventCount", "conversions", "totalRevenue",
                "userEngagementDuration"}},
        {"traffic_acquisition_session_source_report",
            new String[]{"date", "sessionSource"},
            new String[]{"totalUsers", "sessions", "engagedSessions", "eventsPerSession",
                "engagementRate", "eventCount", "conversions", "totalRevenue",
                "userEngagementDuration"}},
        {"traffic_acquisition_session_campaign_report",
            new String[]{"date", "sessionCampaignName"},
            new String[]{"totalUsers", "sessions", "engagedSessions", "eventsPerSession",
                "engagementRate", "eventCount", "conversions", "totalRevenue",
                "userEngagementDuration"}},
        {"traffic_acquisition_session_default_channel_grouping_report",
            new String[]{"date", "sessionDefaultChannelGrouping"},
            new String[]{"totalUsers", "sessions", "engagedSessions", "eventsPerSession",
                "engagementRate", "eventCount", "conversions", "totalRevenue",
                "userEngagementDuration"}},
        {"traffic_acquisition_session_source_platform_report",
            new String[]{"date", "sessionSourcePlatform"},
            new String[]{"totalUsers", "sessions", "engagedSessions", "eventsPerSession",
                "engagementRate", "eventCount", "conversions", "totalRevenue",
                "userEngagementDuration"}},
        {"events_report",
            new String[]{"date", "eventName"},
            new String[]{"eventCount", "totalUsers", "eventCountPerUser", "totalRevenue"}},
        {"weekly_events_report",
            new String[]{"yearWeek", "eventName"},
            new String[]{"eventCount", "totalUsers", "eventCountPerUser", "totalRevenue"}},
        {"conversions_report",
            new String[]{"date", "eventName"},
            new String[]{"conversions", "totalUsers", "totalRevenue"}},
        {"pages_title_and_screen_class_report",
            new String[]{"date", "unifiedScreenClass"},
            new String[]{"screenPageViews", "totalUsers", "newUsers", "eventCount",
                "conversions", "totalRevenue", "userEngagementDuration"}},
        {"pages_path_report",
            new String[]{"date", "pagePath"},
            new String[]{"screenPageViews", "totalUsers", "newUsers", "eventCount",
                "conversions", "totalRevenue", "userEngagementDuration"}},
        {"pages_title_and_screen_name_report",
            new String[]{"date", "unifiedScreenName"},
            new String[]{"screenPageViews", "totalUsers", "newUsers", "eventCount",
                "conversions", "totalRevenue", "userEngagementDuration"}},
        {"content_group_report",
            new String[]{"date", "contentGroup"},
            new String[]{"screenPageViews", "totalUsers", "newUsers", "eventCount",
                "conversions", "totalRevenue", "userEngagementDuration"}},
        {"ecommerce_purchases_item_name_report",
            new String[]{"date", "itemName"},
            new String[]{"cartToViewRate", "purchaseToViewRate", "itemsPurchased",
                "itemRevenue", "itemsAddedToCart", "itemsViewed"}},
        {"ecommerce_purchases_item_id_report",
            new String[]{"date", "itemId"},
            new String[]{"cartToViewRate", "purchaseToViewRate", "itemsPurchased",
                "itemRevenue", "itemsAddedToCart", "itemsViewed"}},
        {"ecommerce_purchases_item_category_report_combined",
            new String[]{"date", "itemCategory", "itemCategory2", "itemCategory3",
                "itemCategory4", "itemCategory5"},
            new String[]{"cartToViewRate", "purchaseToViewRate", "itemsPurchased",
                "itemRevenue", "itemsAddedToCart", "itemsViewed"}},
        {"ecommerce_purchases_item_category_report",
            new String[]{"date", "itemCategory"},
            new String[]{"cartToViewRate", "purchaseToViewRate", "itemsPurchased",
                "itemRevenue", "itemsAddedToCart", "itemsViewed"}},
        {"ecommerce_purchases_item_category_2_report",
            new String[]{"date", "itemCategory2"},
            new String[]{"cartToViewRate", "purchaseToViewRate", "itemsPurchased",
                "itemRevenue", "itemsAddedToCart", "itemsViewed"}},
        {"ecommerce_purchases_item_category_3_report",
            new String[]{"date", "itemCategory3"},
            new String[]{"cartToViewRate", "purchaseToViewRate", "itemsPurchased",
                "itemRevenue", "itemsAddedToCart", "itemsViewed"}},
        {"ecommerce_purchases_item_category_4_report",
            new String[]{"date", "itemCategory4"},
            new String[]{"cartToViewRate", "purchaseToViewRate", "itemsPurchased",
                "itemRevenue", "itemsAddedToCart", "itemsViewed"}},
        {"ecommerce_purchases_item_category_5_report",
            new String[]{"date", "itemCategory5"},
            new String[]{"cartToViewRate", "purchaseToViewRate", "itemsPurchased",
                "itemRevenue", "itemsAddedToCart", "itemsViewed"}},
        {"ecommerce_purchases_item_brand_report",
            new String[]{"date", "itemBrand"},
            new String[]{"cartToViewRate", "purchaseToViewRate", "itemsPurchased",
                "itemRevenue", "itemsAddedToCart", "itemsViewed"}},
        {"publisher_ads_ad_unit_report",
            new String[]{"date", "adUnitName"},
            new String[]{"publisherAdImpressions", "adUnitExposure", "publisherAdClicks",
                "totalAdRevenue"}},
        {"publisher_ads_page_path_report",
            new String[]{"date", "pagePath"},
            new String[]{"publisherAdImpressions", "adUnitExposure", "publisherAdClicks",
                "totalAdRevenue"}},
        {"publisher_ads_ad_format_report",
            new String[]{"date", "adFormat"},
            new String[]{"publisherAdImpressions", "adUnitExposure", "publisherAdClicks",
                "totalAdRevenue"}},
        {"publisher_ads_ad_source_report",
            new String[]{"date", "adSourceName"},
            new String[]{"publisherAdImpressions", "adUnitExposure", "publisherAdClicks",
                "totalAdRevenue"}},
        {"demographic_country_report",
            new String[]{"date", "country"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "conversions", "totalRevenue"}},
        {"demographic_region_report",
            new String[]{"date", "region"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "conversions", "totalRevenue"}},
        {"demographic_city_report",
            new String[]{"date", "city"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "conversions", "totalRevenue"}},
        {"demographic_language_report",
            new String[]{"date", "language"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "conversions", "totalRevenue"}},
        {"demographic_age_report",
            new String[]{"date", "userAgeBracket"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "conversions", "totalRevenue"}},
        {"demographic_gender_report",
            new String[]{"date", "userGender"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "conversions", "totalRevenue"}},
        {"demographic_interest_report",
            new String[]{"date", "brandingInterest"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "conversions", "totalRevenue"}},
        {"tech_browser_report",
            new String[]{"date", "browser"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "eventCount", "conversions", "totalRevenue"}},
        {"tech_device_category_report",
            new String[]{"date", "deviceCategory"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "eventCount", "conversions", "totalRevenue"}},
        {"tech_device_model_report",
            new String[]{"date", "deviceModel"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "eventCount", "conversions", "totalRevenue"}},
        {"tech_screen_resolution_report",
            new String[]{"date", "screenResolution"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "eventCount", "conversions", "totalRevenue"}},
        {"tech_app_version_report",
            new String[]{"date", "appVersion"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "eventCount", "conversions", "totalRevenue"}},
        {"tech_platform_report",
            new String[]{"date", "platform"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "eventCount", "conversions", "totalRevenue"}},
        {"tech_platform_device_category_report",
            new String[]{"date", "platform", "deviceCategory"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "eventCount", "conversions", "totalRevenue"}},
        {"tech_operating_system_report",
            new String[]{"date", "operatingSystem"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "eventCount", "conversions", "totalRevenue"}},
        {"tech_os_with_version_report",
            new String[]{"date", "operatingSystemWithVersion"},
            new String[]{"totalUsers", "newUsers", "engagedSessions", "engagementRate",
                "eventCount", "conversions", "totalRevenue"}},
    };

    private GoogleAnalyticsDataApiTaskBody() {
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
        cls.addMethod(buildJwt());
        cls.addMethod(runReport());
        cls.addMethod(extractRecords());
        cls.addMethod(sanitizeTopic());
        cls.addMethod(sendWithRetry());
        cls.addMethod(stop());
        cls.addMethod(buildDefaultReports());
        return cls.build();
    }

    private static void addFields(TypeSpec.Builder cls, ClassName configClass) {
        cls.addField(FieldSpec.builder(OBJECT_MAPPER, "MAPPER",
                Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("new $T()", OBJECT_MAPPER).build());
        cls.addField(FieldSpec.builder(LIST_OBJECT_ARRAY, "HARDCODED_REPORTS",
                Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("buildDefaultReports()").build());
        cls.addField(configClass, "config", Modifier.PRIVATE);
        cls.addField(HTTP_CLIENT, "httpClient", Modifier.PRIVATE);
        cls.addField(LIST_OBJECT_ARRAY, "reports", Modifier.PRIVATE);
        cls.addField(LIST_STRING, "propertyIds", Modifier.PRIVATE);
        cls.addField(MAP_STRING_OBJECT, "credentials", Modifier.PRIVATE);
        cls.addField(FieldSpec.builder(String.class, "credentialsType",
            Modifier.PRIVATE).build());
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
            .addCode(""
                + "this.config = new $T(props);\n"
                + "this.httpClient = $T.newHttpClient();\n"
                + "String propIdsRaw = config.getPropertyIds();\n"
                + "if (propIdsRaw == null || propIdsRaw.isBlank()) {\n"
                + "    throw new $T(\"`property_ids` config is required\");\n"
                + "}\n"
                + "try {\n"
                + "    this.propertyIds = MAPPER.readValue(propIdsRaw, $T.class);\n"
                + "} catch (Exception e) {\n"
                + "    throw new $T(\"`property_ids` must be a JSON array\", e);\n"
                + "}\n"
                + "String customRaw = config.getCustomReportsArray();\n"
                + "if (customRaw != null && !customRaw.isBlank()) {\n"
                + "    try {\n"
                + "        $T<$T<String, Object>> custom = MAPPER.readValue(customRaw, $T.class);\n"
                + "        this.reports = new $T<>();\n"
                + "        for ($T<String, Object> r : custom) {\n"
                + "            $T<?> dims = r.get($S) instanceof $T<?> dl ? dl : $T.of();\n"
                + "            $T<?> mets = r.get($S) instanceof $T<?> ml ? ml : $T.of();\n"
                + "            this.reports.add(new $T[]{\n"
                + "                r.get($S),\n"
                + "                dims.stream().map($T::toString).toArray($T[]::new),\n"
                + "                mets.stream().map($T::toString).toArray($T[]::new)\n"
                + "            });\n"
                + "        }\n"
                + "    } catch (Exception e) {\n"
                + "        throw new $T(\"Invalid `custom_reports_array`\", e);\n"
                + "    }\n"
                + "} else {\n"
                + "    this.reports = HARDCODED_REPORTS;\n"
                + "}\n"
                + "String credsJson = config.getCredentials();\n"
                + "if (credsJson == null || credsJson.isBlank()) {\n"
                + "    throw new $T(\"`credentials` config is required\");\n"
                + "}\n"
                + "try {\n"
                + "    this.credentials = MAPPER.readValue(credsJson, $T.class);\n"
                + "    this.credentialsType = $T.valueOf(\n"
                + "        credentials.getOrDefault($S, $S));\n"
                + "} catch (Exception e) {\n"
                + "    throw new $T(\"`credentials` must be valid JSON\", e);\n"
                + "}\n",
                configClass, HTTP_CLIENT, CONNECT_EXCEPTION,
                List.class, CONNECT_EXCEPTION,
                List.class, Map.class, List.class,
                ArrayList.class,
                Map.class,
                List.class, "dimensions", List.class, List.class,
                List.class, "metrics", List.class, List.class,
                Object.class,
                "name",
                Object.class, String.class,
                Object.class, String.class,
                CONNECT_EXCEPTION,
                CONNECT_EXCEPTION,
                Map.class,
                String.class, "auth_type", "Client",
                CONNECT_EXCEPTION)
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
                + "for (int pi = 0; pi < propertyIds.size(); pi++) {\n"
                + "    String propId = propertyIds.get(pi);\n"
                + "    for ($T[] report : reports) {\n"
                + "        String name = ($T) report[0];\n"
                + "        $T[] dims = ($T[]) report[1];\n"
                + "        $T[] mets = ($T[]) report[2];\n"
                + "        String topic = pi == 0 ? sanitizeTopic(name)\n"
                + "            : sanitizeTopic(name + $S + propId);\n"
                + "        try {\n"
                + "            records.addAll(runReport(propId, topic, dims, mets));\n"
                + "        } catch (Exception e) {\n"
                + "            throw new $T(\"GA4 report failed: \" + name, e);\n"
                + "        }\n"
                + "    }\n"
                + "}\n"
                + "if (records.isEmpty()) Thread.sleep(30000L);\n"
                + "return records;\n",
                List.class, SOURCE_RECORD, ArrayList.class,
                Object.class,
                String.class,
                String.class, String.class,
                String.class, String.class,
                "Property",
                CONNECT_EXCEPTION)
            .build();
    }

    private static MethodSpec getAccessToken() {
        return MethodSpec.methodBuilder("getAccessToken")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class)
            .addException(Exception.class)
            .addCode(""
                + "if (accessToken != null && System.currentTimeMillis() < tokenExpiryMs - 30000L) {\n"
                + "    return accessToken;\n"
                + "}\n"
                + "String body;\n"
                + "String tokenUrl;\n"
                + "if ($S.equals(credentialsType)) {\n"
                + "    String sjStr = $T.valueOf(credentials.get($S));\n"
                + "    $T<String, Object> sa = MAPPER.readValue(sjStr, $T.class);\n"
                + "    String email = $T.valueOf(sa.get($S));\n"
                + "    String privKey = $T.valueOf(sa.get($S));\n"
                + "    tokenUrl = $T.valueOf(sa.getOrDefault($S, $S));\n"
                + "    String jwt = buildJwt(email, privKey, tokenUrl);\n"
                + "    body = $S\n"
                + "        + $T.encode(jwt, $T.UTF_8);\n"
                + "} else {\n"
                + "    String clientId = $T.valueOf(credentials.get($S));\n"
                + "    String clientSecret = $T.valueOf(credentials.get($S));\n"
                + "    String refreshToken = $T.valueOf(credentials.get($S));\n"
                + "    body = $S\n"
                + "        + $T.encode(clientId, $T.UTF_8)\n"
                + "        + $S + $T.encode(clientSecret, $T.UTF_8)\n"
                + "        + $S + $T.encode(refreshToken, $T.UTF_8);\n"
                + "    tokenUrl = $S;\n"
                + "}\n"
                + "$T req = $T.newBuilder()\n"
                + "    .uri($T.create(tokenUrl))\n"
                + "    .header($S, $S)\n"
                + "    .POST($T.BodyPublishers.ofString(body))\n"
                + "    .build();\n"
                + "$T<String> resp = httpClient.send(req, $T.BodyHandlers.ofString());\n"
                + "if (resp.statusCode() / 100 != 2) {\n"
                + "    throw new $T($S + resp.statusCode() + $S + resp.body());\n"
                + "}\n"
                + "$T<String, Object> tok = MAPPER.readValue(resp.body(), $T.class);\n"
                + "Object tokenObj = tok.get($S);\n"
                + "if (tokenObj == null) {\n"
                + "    throw new $T($S + resp.body());\n"
                + "}\n"
                + "this.accessToken = $T.valueOf(tokenObj);\n"
                + "Object expiresIn = tok.getOrDefault($S, 3600);\n"
                + "long expSec = (expiresIn instanceof Number n) ? n.longValue() : 3600L;\n"
                + "this.tokenExpiryMs = System.currentTimeMillis() + expSec * 1000L;\n"
                + "return this.accessToken;\n",
                "Service",
                String.class, "credentials_json",
                Map.class, Map.class,
                String.class, "client_email",
                String.class, "private_key",
                String.class, "token_uri", "https://oauth2.googleapis.com/token",
                "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=",
                URL_ENCODER, STANDARD_CHARSETS,
                String.class, "client_id",
                String.class, "client_secret",
                String.class, "refresh_token",
                "grant_type=refresh_token&client_id=",
                URL_ENCODER, STANDARD_CHARSETS,
                "&client_secret=", URL_ENCODER, STANDARD_CHARSETS,
                "&refresh_token=", URL_ENCODER, STANDARD_CHARSETS,
                "https://www.googleapis.com/oauth2/v4/token",
                HTTP_REQUEST, HTTP_REQUEST,
                URI_CLASS,
                "Content-Type", "application/x-www-form-urlencoded",
                HTTP_REQUEST,
                HTTP_RESPONSE, HTTP_RESPONSE,
                CONNECT_EXCEPTION, "GA4 token refresh failed: ", " ",
                Map.class, Map.class,
                "access_token",
                CONNECT_EXCEPTION, "GA4 token response missing access_token: ",
                String.class,
                "expires_in")
            .build();
    }

    private static MethodSpec buildJwt() {
        return MethodSpec.methodBuilder("buildJwt")
            .addModifiers(Modifier.PRIVATE)
            .returns(String.class)
            .addParameter(String.class, "clientEmail")
            .addParameter(String.class, "privateKeyPem")
            .addParameter(String.class, "tokenUri")
            .addException(Exception.class)
            .addCode(""
                + "String key = privateKeyPem\n"
                + "    .replace($S, $S)\n"
                + "    .replace($S, $S)\n"
                + "    .replaceAll($S, $S);\n"
                + "byte[] keyBytes = $T.getDecoder().decode(key);\n"
                + "$T kf = $T.getInstance($S);\n"
                + "$T pkcsSpec = new $T(keyBytes);\n"
                + "$T pk = kf.generatePrivate(pkcsSpec);\n"
                + "long now = System.currentTimeMillis() / 1000L;\n"
                + "String header = $T.getUrlEncoder().withoutPadding().encodeToString(\n"
                + "    $S.getBytes($T.UTF_8));\n"
                + "String payload = $T.getUrlEncoder().withoutPadding().encodeToString(\n"
                + "    $T.format($S, clientEmail, clientEmail, tokenUri, now, now + 3600L)\n"
                + "    .getBytes($T.UTF_8));\n"
                + "String sigInput = header + $S + payload;\n"
                + "$T sig = $T.getInstance($S);\n"
                + "sig.initSign(pk);\n"
                + "sig.update(sigInput.getBytes($T.UTF_8));\n"
                + "return sigInput + $S + $T.getUrlEncoder().withoutPadding()\n"
                + "    .encodeToString(sig.sign());\n",
                "-----BEGIN PRIVATE KEY-----", "",
                "-----END PRIVATE KEY-----", "",
                "\\s", "",
                BASE64,
                KEY_FACTORY, KEY_FACTORY, "RSA",
                PKCS8_SPEC, PKCS8_SPEC,
                PRIVATE_KEY,
                BASE64,
                "{\"alg\":\"RS256\",\"typ\":\"JWT\"}", STANDARD_CHARSETS,
                BASE64,
                String.class,
                "{\"iss\":\"%s\",\"sub\":\"%s\",\"aud\":\"%s\","
                    + "\"scope\":\"https://www.googleapis.com/auth/analytics.readonly\","
                    + "\"iat\":%d,\"exp\":%d}",
                STANDARD_CHARSETS,
                ".",
                JDK_SIGNATURE, JDK_SIGNATURE, "SHA256withRSA",
                STANDARD_CHARSETS,
                ".", BASE64)
            .build();
    }

    private static MethodSpec runReport() {
        return MethodSpec.methodBuilder("runReport")
            .addModifiers(Modifier.PRIVATE)
            .returns(LIST_SOURCE_RECORD)
            .addParameter(String.class, "propertyId")
            .addParameter(String.class, "topic")
            .addParameter(ArrayTypeName.of(String.class), "dims")
            .addParameter(ArrayTypeName.of(String.class), "mets")
            .addException(Exception.class)
            .addCode(""
                + "$T body = MAPPER.createObjectNode();\n"
                + "$T dimsArr = body.putArray($S);\n"
                + "for (String d : dims) dimsArr.addObject().put($S, d);\n"
                + "$T metsArr = body.putArray($S);\n"
                + "for (String m : mets) metsArr.addObject().put($S, m);\n"
                + "$T dr = body.putArray($S).addObject();\n"
                + "String startDate = config.getDateRangesStartDate();\n"
                + "if (startDate == null || startDate.isBlank()) {\n"
                + "    startDate = $T.now().minusDays(730).toString();\n"
                + "}\n"
                + "String endDate = config.getDateRangesEndDate();\n"
                + "if (endDate == null || endDate.isBlank()) {\n"
                + "    endDate = $T.now().toString();\n"
                + "}\n"
                + "dr.put($S, startDate);\n"
                + "dr.put($S, endDate);\n"
                + "body.put($S, 100000);\n"
                + "body.put($S, true);\n"
                + "$T<$T> records = new $T<>();\n"
                + "long rowOffset = 0;\n"
                + "long totalRows;\n"
                + "String url = $S + propertyId + $S;\n"
                + "do {\n"
                + "    if (rowOffset > 0) body.put($S, rowOffset);\n"
                + "    $T req = $T.newBuilder()\n"
                + "        .uri($T.create(url))\n"
                + "        .header($S, $S + getAccessToken())\n"
                + "        .header($S, $S)\n"
                + "        .POST($T.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))\n"
                + "        .build();\n"
                + "    $T<String> resp = sendWithRetry(req);\n"
                + "    if (resp.statusCode() / 100 != 2) {\n"
                + "        throw new $T($S + propertyId + $S\n"
                + "            + resp.statusCode() + $S + resp.body());\n"
                + "    }\n"
                + "    $T root = MAPPER.readTree(resp.body());\n"
                + "    totalRows = root.path($S).asLong(0);\n"
                + "    records.addAll(extractRecords(root, propertyId, topic));\n"
                + "    rowOffset += 100000;\n"
                + "} while (rowOffset < totalRows);\n"
                + "return records;\n",
                OBJECT_NODE,
                ARRAY_NODE, "dimensions", "name",
                ARRAY_NODE, "metrics", "name",
                OBJECT_NODE, "dateRanges",
                LOCAL_DATE,
                LOCAL_DATE,
                "startDate", "endDate",
                "limit", "returnPropertyQuota",
                List.class, SOURCE_RECORD, ArrayList.class,
                "https://analyticsdata.googleapis.com/v1beta/properties/", ":runReport",
                "offset",
                HTTP_REQUEST, HTTP_REQUEST,
                URI_CLASS,
                "Authorization", "Bearer ",
                "Content-Type", "application/json",
                HTTP_REQUEST,
                HTTP_RESPONSE,
                CONNECT_EXCEPTION, "GA4 runReport failed for property ", ": ",
                " ", JSON_NODE,
                "rowCount")
            .build();
    }

    private static MethodSpec extractRecords() {
        return MethodSpec.methodBuilder("extractRecords")
            .addModifiers(Modifier.PRIVATE)
            .returns(LIST_SOURCE_RECORD)
            .addParameter(JSON_NODE, "response")
            .addParameter(String.class, "propertyId")
            .addParameter(String.class, "topic")
            .addException(Exception.class)
            .addCode(""
                + "$T<String> dimNames = new $T<>();\n"
                + "for ($T h : response.path($S)) dimNames.add(h.path($S).asText($S));\n"
                + "$T<String> metNames = new $T<>();\n"
                + "for ($T h : response.path($S)) metNames.add(h.path($S).asText($S));\n"
                + "$T<$T> records = new $T<>();\n"
                + "$T<String, Object> srcPart = $T.singletonMap($S, propertyId);\n"
                + "$T rows = response.path($S);\n"
                + "if (!rows.isArray()) return records;\n"
                + "int idx = 0;\n"
                + "for ($T row : rows) {\n"
                + "    $T<String, Object> rec = new $T<>();\n"
                + "    $T dimVals = row.path($S);\n"
                + "    for (int i = 0; i < dimNames.size() && i < dimVals.size(); i++) {\n"
                + "        rec.put(dimNames.get(i), dimVals.get(i).path($S).asText($S));\n"
                + "    }\n"
                + "    $T metVals = row.path($S);\n"
                + "    for (int i = 0; i < metNames.size() && i < metVals.size(); i++) {\n"
                + "        rec.put(metNames.get(i), metVals.get(i).path($S).asText($S));\n"
                + "    }\n"
                + "    rec.put($S, propertyId);\n"
                + "    $T<String, Object> srcOff = $T.singletonMap($S, (long) idx);\n"
                + "    records.add(new $T(srcPart, srcOff, topic,\n"
                + "        $T.STRING_SCHEMA, $T.valueOf(idx),\n"
                + "        $T.STRING_SCHEMA, MAPPER.writeValueAsString(rec)));\n"
                + "    idx++;\n"
                + "}\n"
                + "return records;\n",
                List.class, ArrayList.class,
                JSON_NODE, "dimensionHeaders", "name", "",
                List.class, ArrayList.class,
                JSON_NODE, "metricHeaders", "name", "",
                List.class, SOURCE_RECORD, ArrayList.class,
                Map.class, Collections.class, "property_id",
                JSON_NODE, "rows",
                JSON_NODE,
                Map.class, LinkedHashMap.class,
                JSON_NODE, "dimensionValues",
                "value", "",
                JSON_NODE, "metricValues",
                "value", "",
                "property_id",
                Map.class, Collections.class, "idx",
                SOURCE_RECORD,
                SCHEMA, String.class,
                SCHEMA)
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

    private static MethodSpec buildDefaultReports() {
        MethodSpec.Builder m = MethodSpec.methodBuilder("buildDefaultReports")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .returns(LIST_OBJECT_ARRAY);
        m.addStatement("$T<$T[]> r = new $T<>()", List.class, Object.class, ArrayList.class);
        for (Object[] rpt : REPORTS) {
            String name = (String) rpt[0];
            String[] dims = (String[]) rpt[1];
            String[] mets = (String[]) rpt[2];
            StringBuilder d = new StringBuilder();
            for (int i = 0; i < dims.length; i++) {
                if (i > 0) d.append(", ");
                d.append('"').append(dims[i]).append('"');
            }
            StringBuilder me = new StringBuilder();
            for (int i = 0; i < mets.length; i++) {
                if (i > 0) me.append(", ");
                me.append('"').append(mets[i]).append('"');
            }
            m.addCode("r.add(new $T[]{$S, new $T[]{" + d + "}, new $T[]{" + me + "}});\n",
                Object.class, name, String.class, String.class);
        }
        m.addStatement("return r");
        return m.build();
    }
}
