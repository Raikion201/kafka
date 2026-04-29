package io.kafka.connect.generated;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public final class AkeneoSourceTask extends SourceTask {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private AkeneoConnectorConfig config;

  private HttpClient httpClient;

  private volatile String cachedSessionToken;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new AkeneoConnectorConfig(props);
    this.httpClient = HttpClient.newHttpClient();
    try {
      loginAndCacheSessionToken();
    } catch (Exception e) {
      throw new ConnectException("Failed to obtain session token", e);
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> all = new ArrayList<>();
    all.addAll(pollProducts());
    all.addAll(pollCategories());
    all.addAll(pollFamilies());
    all.addAll(pollFamilyVariants());
    all.addAll(pollAttributes());
    all.addAll(pollAttributeGroups());
    all.addAll(pollAssociationTypes());
    all.addAll(pollChannels());
    all.addAll(pollLocales());
    all.addAll(pollCurrencies());
    all.addAll(pollMeasureFamilies());
    return all;
  }

  private List<SourceRecord> pollProducts() throws InterruptedException {
    final String streamName = "products";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("{{ config[\"host\"] }}/api/rest/v1/products-uuid");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&limit=" + 100);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + cachedSessionToken)
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("_embedded");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("items");
        if (current == null) {
          return allRecords;
        }
        json = current;
        List<Object> records;
        if (json instanceof List) {
          records = (List<Object>) json;
        } else {
          records = Collections.singletonList(json);
        }
        for (Object record : records) {
          String value = MAPPER.writeValueAsString(record);
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("page", page), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        page++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollCategories() throws InterruptedException {
    final String streamName = "categories ";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("{{ config[\"host\"] }}/api/rest/v1/categories");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&limit=" + 100);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + cachedSessionToken)
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("_embedded");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("items");
        if (current == null) {
          return allRecords;
        }
        json = current;
        List<Object> records;
        if (json instanceof List) {
          records = (List<Object>) json;
        } else {
          records = Collections.singletonList(json);
        }
        for (Object record : records) {
          String value = MAPPER.writeValueAsString(record);
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("page", page), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        page++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollFamilies() throws InterruptedException {
    final String streamName = "families";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("{{ config[\"host\"] }}/api/rest/v1/families");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&limit=" + 100);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + cachedSessionToken)
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("_embedded");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("items");
        if (current == null) {
          return allRecords;
        }
        json = current;
        List<Object> records;
        if (json instanceof List) {
          records = (List<Object>) json;
        } else {
          records = Collections.singletonList(json);
        }
        for (Object record : records) {
          String value = MAPPER.writeValueAsString(record);
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("page", page), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        page++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollFamilyVariants() throws InterruptedException {
    final String streamName = "family_variants";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("{{ config[\"host\"] }}/api/rest/v1/families/{{ stream_partition.family_code }}/variants");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&limit=" + 100);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + cachedSessionToken)
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("_embedded");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("items");
        if (current == null) {
          return allRecords;
        }
        json = current;
        List<Object> records;
        if (json instanceof List) {
          records = (List<Object>) json;
        } else {
          records = Collections.singletonList(json);
        }
        for (Object record : records) {
          String value = MAPPER.writeValueAsString(record);
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("page", page), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        page++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollAttributes() throws InterruptedException {
    final String streamName = "attributes";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("{{ config[\"host\"] }}/api/rest/v1/attributes");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&limit=" + 100);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + cachedSessionToken)
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("_embedded");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("items");
        if (current == null) {
          return allRecords;
        }
        json = current;
        List<Object> records;
        if (json instanceof List) {
          records = (List<Object>) json;
        } else {
          records = Collections.singletonList(json);
        }
        for (Object record : records) {
          String value = MAPPER.writeValueAsString(record);
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("page", page), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        page++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollAttributeGroups() throws InterruptedException {
    final String streamName = "attribute_groups";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("{{ config[\"host\"] }}/api/rest/v1/attribute-groups");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&limit=" + 100);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + cachedSessionToken)
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("_embedded");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("items");
        if (current == null) {
          return allRecords;
        }
        json = current;
        List<Object> records;
        if (json instanceof List) {
          records = (List<Object>) json;
        } else {
          records = Collections.singletonList(json);
        }
        for (Object record : records) {
          String value = MAPPER.writeValueAsString(record);
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("page", page), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        page++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollAssociationTypes() throws InterruptedException {
    final String streamName = "association_types";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("{{ config[\"host\"] }}/api/rest/v1/association-types");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&limit=" + 100);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + cachedSessionToken)
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("_embedded");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("items");
        if (current == null) {
          return allRecords;
        }
        json = current;
        List<Object> records;
        if (json instanceof List) {
          records = (List<Object>) json;
        } else {
          records = Collections.singletonList(json);
        }
        for (Object record : records) {
          String value = MAPPER.writeValueAsString(record);
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("page", page), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        page++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollChannels() throws InterruptedException {
    final String streamName = "channels";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("{{ config[\"host\"] }}/api/rest/v1/channels");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&limit=" + 100);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + cachedSessionToken)
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("_embedded");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("items");
        if (current == null) {
          return allRecords;
        }
        json = current;
        List<Object> records;
        if (json instanceof List) {
          records = (List<Object>) json;
        } else {
          records = Collections.singletonList(json);
        }
        for (Object record : records) {
          String value = MAPPER.writeValueAsString(record);
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("page", page), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        page++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollLocales() throws InterruptedException {
    final String streamName = "locales";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("{{ config[\"host\"] }}/api/rest/v1/locales");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&limit=" + 100);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + cachedSessionToken)
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("_embedded");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("items");
        if (current == null) {
          return allRecords;
        }
        json = current;
        List<Object> records;
        if (json instanceof List) {
          records = (List<Object>) json;
        } else {
          records = Collections.singletonList(json);
        }
        for (Object record : records) {
          String value = MAPPER.writeValueAsString(record);
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("page", page), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        page++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollCurrencies() throws InterruptedException {
    final String streamName = "currencies";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("{{ config[\"host\"] }}/api/rest/v1/currencies");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&limit=" + 100);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + cachedSessionToken)
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("_embedded");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("items");
        if (current == null) {
          return allRecords;
        }
        json = current;
        List<Object> records;
        if (json instanceof List) {
          records = (List<Object>) json;
        } else {
          records = Collections.singletonList(json);
        }
        for (Object record : records) {
          String value = MAPPER.writeValueAsString(record);
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("page", page), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        page++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollMeasureFamilies() throws InterruptedException {
    final String streamName = "measure_families";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("{{ config[\"host\"] }}/api/rest/v1/measure-families");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&limit=" + 100);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + cachedSessionToken)
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("_embedded");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("items");
        if (current == null) {
          return allRecords;
        }
        json = current;
        List<Object> records;
        if (json instanceof List) {
          records = (List<Object>) json;
        } else {
          records = Collections.singletonList(json);
        }
        for (Object record : records) {
          String value = MAPPER.writeValueAsString(record);
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("page", page), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        page++;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private void loginAndCacheSessionToken() throws Exception {
    Map<String, String> bodyMap = new LinkedHashMap<>();
    bodyMap.put("password", config.getPassword());
    bodyMap.put("username", config.getApiUsername());
    bodyMap.put("grant_type", "password");
    String loginBody = MAPPER.writeValueAsString(bodyMap);
    HttpRequest loginReq = HttpRequest.newBuilder()
                .uri(URI.create(config.getHost() + "/api/oauth/v1/token"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
                        (config.getClientId() + ":" + config.getSecret()).getBytes(StandardCharsets.UTF_8)))
                .POST(HttpRequest.BodyPublishers.ofString(loginBody))
                .build();
    HttpResponse<String> loginResp = httpClient.send(loginReq, HttpResponse.BodyHandlers.ofString());
    Object loginJson = MAPPER.readValue(loginResp.body(), Object.class);
    Object tokenStep = loginJson;
    if (tokenStep instanceof Map) {
      tokenStep = ((Map<?, ?>) tokenStep).get("access_token");
    } else {
      tokenStep = null;
    }
    if (tokenStep == null) {
      throw new ConnectException("Session token not found in login response");
    }
    cachedSessionToken = String.valueOf(tokenStep);
  }

  private HttpResponse<String> sendWithRetry(HttpRequest request) throws Exception {
    int attempt = 0;
    while (true) {
      HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if ((resp.statusCode() == 429 || resp.statusCode() >= 500) && attempt < 3) {
        Thread.sleep(1000L << attempt);
        attempt++;
        continue;
      }
      return resp;
    }
  }

  @Override
  public void stop() {
    httpClient = null;
  }
}
