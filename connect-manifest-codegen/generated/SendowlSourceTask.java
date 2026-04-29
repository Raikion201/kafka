package io.kafka.connect.generated;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public final class SendowlSourceTask extends SourceTask {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SendowlConnectorConfig config;

  private HttpClient httpClient;

  private String cachedCredentials;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new SendowlConnectorConfig(props);
    this.httpClient = HttpClient.newHttpClient();
    this.cachedCredentials = Base64.getEncoder().encodeToString(
                (config.getUsername() + ":" + config.getPassword()).getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> all = new ArrayList<>();
    all.addAll(pollProducts());
    all.addAll(pollPackages());
    all.addAll(pollOrders());
    all.addAll(pollSubscriptions());
    return all;
  }

  private List<SourceRecord> pollProducts() throws InterruptedException {
    final String streamName = "products";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://www.sendowl.com/api/v1/products");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&per_page=" + 50);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Basic " + cachedCredentials)
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
        current = ((Map<?, ?>) current).get("product");
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

  private List<SourceRecord> pollPackages() throws InterruptedException {
    final String streamName = "packages";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://www.sendowl.com/api/v1/packages");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&per_page=" + 50);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Basic " + cachedCredentials)
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
        current = ((Map<?, ?>) current).get("package");
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

  private List<SourceRecord> pollOrders() throws InterruptedException {
    final String streamName = "orders";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://www.sendowl.com/api/v1/orders");
      urlBuilder.append("?from=" + URLEncoder.encode(config.getStartDate(), StandardCharsets.UTF_8));
      urlBuilder.append("&to=" + URLEncoder.encode(Instant.now().toString(), StandardCharsets.UTF_8));
      urlBuilder.append("&page=" + page);
      urlBuilder.append("&per_page=" + 50);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Basic " + cachedCredentials)
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
        current = ((Map<?, ?>) current).get("order");
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

  private List<SourceRecord> pollSubscriptions() throws InterruptedException {
    final String streamName = "subscriptions";
    List<SourceRecord> allRecords = new ArrayList<>();
    int page = 1;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://www.sendowl.com/api/v1/subscriptions");
      urlBuilder.append("?page=" + page);
      urlBuilder.append("&per_page=" + 50);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Basic " + cachedCredentials)
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
        current = ((Map<?, ?>) current).get("subscription");
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
