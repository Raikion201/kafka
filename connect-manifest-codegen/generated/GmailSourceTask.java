package io.kafka.connect.generated;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public final class GmailSourceTask extends SourceTask {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private GmailConnectorConfig config;

  private HttpClient httpClient;

  private volatile String cachedToken;

  private volatile long tokenExpiryMs = 0L;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new GmailConnectorConfig(props);
    this.httpClient = HttpClient.newHttpClient();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> all = new ArrayList<>();
    all.addAll(pollGmail());
    all.addAll(pollMessages());
    all.addAll(pollMessagesDetails());
    all.addAll(pollThreads());
    all.addAll(pollThreadsDetails());
    all.addAll(pollLabels());
    all.addAll(pollLabelsDetails());
    all.addAll(pollDrafts());
    return all;
  }

  private List<SourceRecord> pollGmail() throws InterruptedException {
    final String streamName = "gmail";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://gmail.googleapis.com/gmail/v1/users/me/profile");
    try {
      String accessToken = refreshAccessToken();
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("Authorization", "Bearer " + accessToken)
                  .GET()
                  .build();
      HttpResponse<String> response = sendWithRetry(request);
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
      }
      Object json = MAPPER.readValue(response.body(), Object.class);
      List<Object> records;
      if (json instanceof List) {
        records = (List<Object>) json;
      } else {
        records = Collections.singletonList(json);
      }
      for (Object record : records) {
        String value = MAPPER.writeValueAsString(record);
        result.add(new SourceRecord(Map.of("stream", streamName), Map.of("position", 0), streamName, Schema.STRING_SCHEMA, value));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while polling " + streamName, e);
    } catch (Exception e) {
      throw new ConnectException("Failed to poll " + streamName, e);
    }
    return result;
  }

  private List<SourceRecord> pollMessages() throws InterruptedException {
    final String streamName = "messages";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://gmail.googleapis.com/gmail/v1/users/me/messages");
    urlBuilder.append("?includeSpamTrash=" + URLEncoder.encode(config.getIncludeSpamAndTrash(), StandardCharsets.UTF_8));
    try {
      String accessToken = refreshAccessToken();
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("Authorization", "Bearer " + accessToken)
                  .GET()
                  .build();
      HttpResponse<String> response = sendWithRetry(request);
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
      }
      Object json = MAPPER.readValue(response.body(), Object.class);
      Object current = json;
      if (!(current instanceof Map)) {
        return Collections.emptyList();
      }
      current = ((Map<?, ?>) current).get("messages");
      if (current == null) {
        return Collections.emptyList();
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
        result.add(new SourceRecord(Map.of("stream", streamName), Map.of("position", 0), streamName, Schema.STRING_SCHEMA, value));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while polling " + streamName, e);
    } catch (Exception e) {
      throw new ConnectException("Failed to poll " + streamName, e);
    }
    return result;
  }

  private List<SourceRecord> pollMessagesDetails() throws InterruptedException {
    final String streamName = "messages_details";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://gmail.googleapis.com/gmail/v1/users/me/messages");
    urlBuilder.append("?includeSpamTrash=" + URLEncoder.encode(config.getIncludeSpamAndTrash(), StandardCharsets.UTF_8));
    try {
      String accessToken = refreshAccessToken();
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("Authorization", "Bearer " + accessToken)
                  .GET()
                  .build();
      HttpResponse<String> response = sendWithRetry(request);
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
      }
      Object json = MAPPER.readValue(response.body(), Object.class);
      Object current = json;
      if (!(current instanceof Map)) {
        return Collections.emptyList();
      }
      current = ((Map<?, ?>) current).get("messages");
      if (current == null) {
        return Collections.emptyList();
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
        result.add(new SourceRecord(Map.of("stream", streamName), Map.of("position", 0), streamName, Schema.STRING_SCHEMA, value));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while polling " + streamName, e);
    } catch (Exception e) {
      throw new ConnectException("Failed to poll " + streamName, e);
    }
    return result;
  }

  private List<SourceRecord> pollThreads() throws InterruptedException {
    final String streamName = "threads";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://gmail.googleapis.com/gmail/v1/users/me/threads");
    urlBuilder.append("?includeSpamTrash=" + URLEncoder.encode(config.getIncludeSpamAndTrash(), StandardCharsets.UTF_8));
    try {
      String accessToken = refreshAccessToken();
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("Authorization", "Bearer " + accessToken)
                  .GET()
                  .build();
      HttpResponse<String> response = sendWithRetry(request);
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
      }
      Object json = MAPPER.readValue(response.body(), Object.class);
      Object current = json;
      if (!(current instanceof Map)) {
        return Collections.emptyList();
      }
      current = ((Map<?, ?>) current).get("threads");
      if (current == null) {
        return Collections.emptyList();
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
        result.add(new SourceRecord(Map.of("stream", streamName), Map.of("position", 0), streamName, Schema.STRING_SCHEMA, value));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while polling " + streamName, e);
    } catch (Exception e) {
      throw new ConnectException("Failed to poll " + streamName, e);
    }
    return result;
  }

  private List<SourceRecord> pollThreadsDetails() throws InterruptedException {
    final String streamName = "threads_details";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://gmail.googleapis.com/gmail/v1/users/me/threads");
    try {
      String accessToken = refreshAccessToken();
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("Authorization", "Bearer " + accessToken)
                  .GET()
                  .build();
      HttpResponse<String> response = sendWithRetry(request);
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
      }
      Object json = MAPPER.readValue(response.body(), Object.class);
      Object current = json;
      if (!(current instanceof Map)) {
        return Collections.emptyList();
      }
      current = ((Map<?, ?>) current).get("threads");
      if (current == null) {
        return Collections.emptyList();
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
        result.add(new SourceRecord(Map.of("stream", streamName), Map.of("position", 0), streamName, Schema.STRING_SCHEMA, value));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while polling " + streamName, e);
    } catch (Exception e) {
      throw new ConnectException("Failed to poll " + streamName, e);
    }
    return result;
  }

  private List<SourceRecord> pollLabels() throws InterruptedException {
    final String streamName = "labels";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://gmail.googleapis.com/gmail/v1/users/me/labels");
    try {
      String accessToken = refreshAccessToken();
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("Authorization", "Bearer " + accessToken)
                  .GET()
                  .build();
      HttpResponse<String> response = sendWithRetry(request);
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
      }
      Object json = MAPPER.readValue(response.body(), Object.class);
      Object current = json;
      if (!(current instanceof Map)) {
        return Collections.emptyList();
      }
      current = ((Map<?, ?>) current).get("labels");
      if (current == null) {
        return Collections.emptyList();
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
        result.add(new SourceRecord(Map.of("stream", streamName), Map.of("position", 0), streamName, Schema.STRING_SCHEMA, value));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while polling " + streamName, e);
    } catch (Exception e) {
      throw new ConnectException("Failed to poll " + streamName, e);
    }
    return result;
  }

  private List<SourceRecord> pollLabelsDetails() throws InterruptedException {
    final String streamName = "labels_details";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://gmail.googleapis.com/gmail/v1/users/me/labels");
    try {
      String accessToken = refreshAccessToken();
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("Authorization", "Bearer " + accessToken)
                  .GET()
                  .build();
      HttpResponse<String> response = sendWithRetry(request);
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
      }
      Object json = MAPPER.readValue(response.body(), Object.class);
      Object current = json;
      if (!(current instanceof Map)) {
        return Collections.emptyList();
      }
      current = ((Map<?, ?>) current).get("labels");
      if (current == null) {
        return Collections.emptyList();
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
        result.add(new SourceRecord(Map.of("stream", streamName), Map.of("position", 0), streamName, Schema.STRING_SCHEMA, value));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while polling " + streamName, e);
    } catch (Exception e) {
      throw new ConnectException("Failed to poll " + streamName, e);
    }
    return result;
  }

  private List<SourceRecord> pollDrafts() throws InterruptedException {
    final String streamName = "drafts";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://gmail.googleapis.com/gmail/v1/users/me/drafts");
    try {
      String accessToken = refreshAccessToken();
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("Authorization", "Bearer " + accessToken)
                  .GET()
                  .build();
      HttpResponse<String> response = sendWithRetry(request);
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new ConnectException("HTTP " + response.statusCode() + " from " + urlBuilder);
      }
      Object json = MAPPER.readValue(response.body(), Object.class);
      Object current = json;
      if (!(current instanceof Map)) {
        return Collections.emptyList();
      }
      current = ((Map<?, ?>) current).get("drafts");
      if (current == null) {
        return Collections.emptyList();
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
        result.add(new SourceRecord(Map.of("stream", streamName), Map.of("position", 0), streamName, Schema.STRING_SCHEMA, value));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while polling " + streamName, e);
    } catch (Exception e) {
      throw new ConnectException("Failed to poll " + streamName, e);
    }
    return result;
  }

  private String refreshAccessToken() {
    if (cachedToken != null && System.currentTimeMillis() < tokenExpiryMs) {
      return cachedToken;
    }
    String reqBody = "grant_type=refresh_token"
                + "&client_id=" + config.getClientId()
                + "&client_secret=" + config.getClientSecret()
                + "&refresh_token=" + config.getClientRefreshToken();
    try {
      HttpRequest tokenRequest = HttpRequest.newBuilder()
                  .uri(URI.create("https://oauth2.googleapis.com/token"))
                  .header("Content-Type", "application/x-www-form-urlencoded")
                  .POST(HttpRequest.BodyPublishers.ofString(reqBody))
                  .build();
      HttpResponse<String> tokenResponse = httpClient.send(tokenRequest, HttpResponse.BodyHandlers.ofString());
      Object rawTokenJson = MAPPER.readValue(tokenResponse.body(), Object.class);
      Map<?, ?> tokenJson = (Map<?, ?>) rawTokenJson;
      Object token = tokenJson.get("access_token");
      if (token == null) {
        throw new ConnectException("No access_token in token refresh response");
      }
      Object expiresInObj = tokenJson.get("expires_in");
      long expiresIn = expiresInObj != null ? Long.parseLong(String.valueOf(expiresInObj)) : 3600L;
      tokenExpiryMs = System.currentTimeMillis() + (expiresIn - 60L) * 1000L;
      cachedToken = String.valueOf(token);
      return cachedToken;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted during token refresh", e);
    } catch (Exception e) {
      throw new ConnectException("Failed to refresh access token", e);
    }
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
    if (httpClient instanceof AutoCloseable ac) {
      try {
        ac.close();
      } catch (Exception ignored) {
      }
    }
    httpClient = null;
  }
}
