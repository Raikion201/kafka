package io.kafka.connect.generated;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public final class IlluminaBasespaceSourceTask extends SourceTask {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private IlluminaBasespaceConnectorConfig config;

  private HttpClient httpClient;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new IlluminaBasespaceConnectorConfig(props);
    this.httpClient = HttpClient.newHttpClient();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> all = new ArrayList<>();
    all.addAll(pollProjects());
    all.addAll(pollRuns());
    all.addAll(pollSamples());
    all.addAll(pollSampleFiles());
    all.addAll(pollRunFiles());
    all.addAll(pollAppsessions());
    all.addAll(pollAppresults());
    all.addAll(pollAppresultsFiles());
    return all;
  }

  private List<SourceRecord> pollProjects() throws InterruptedException {
    final String streamName = "projects";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 1024;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.euw2.sh.basespace.illumina.com/v1pre3/users/current/projects");
      urlBuilder.append("?Offset=" + offset);
      urlBuilder.append("&Limit=" + pageLimit);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + config.getAccessToken())
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
        current = ((Map<?, ?>) current).get("Response");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("Items");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("offset", offset), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        offset += pageLimit;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollRuns() throws InterruptedException {
    final String streamName = "runs";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 1024;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.euw2.sh.basespace.illumina.com/v1pre3/users/current/runs");
      urlBuilder.append("?Offset=" + offset);
      urlBuilder.append("&Limit=" + pageLimit);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + config.getAccessToken())
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
        current = ((Map<?, ?>) current).get("Response");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("Items");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("offset", offset), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        offset += pageLimit;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollSamples() throws InterruptedException {
    final String streamName = "samples";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 1024;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.euw2.sh.basespace.illumina.com/v1pre3/projects/{{ stream_partition['parent_id'] }}/samples");
      urlBuilder.append("?Offset=" + offset);
      urlBuilder.append("&Limit=" + pageLimit);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + config.getAccessToken())
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
        current = ((Map<?, ?>) current).get("Response");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("Items");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("offset", offset), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        offset += pageLimit;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollSampleFiles() throws InterruptedException {
    final String streamName = "sample_files";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 1024;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.euw2.sh.basespace.illumina.com/v1pre3/samples/{{ stream_partition['parent_id'] }}/files");
      urlBuilder.append("?Offset=" + offset);
      urlBuilder.append("&Limit=" + pageLimit);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + config.getAccessToken())
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
        current = ((Map<?, ?>) current).get("Response");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("Items");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("offset", offset), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        offset += pageLimit;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollRunFiles() throws InterruptedException {
    final String streamName = "run_files";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 1024;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.euw2.sh.basespace.illumina.com/v1pre3/runs/{{ stream_partition['parent_id'] }}/files");
      urlBuilder.append("?Offset=" + offset);
      urlBuilder.append("&Limit=" + pageLimit);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + config.getAccessToken())
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
        current = ((Map<?, ?>) current).get("Response");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("Items");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("offset", offset), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        offset += pageLimit;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollAppsessions() throws InterruptedException {
    final String streamName = "appsessions";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 1024;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.euw2.sh.basespace.illumina.com/v1pre3/users/current/appsessions");
      urlBuilder.append("?Offset=" + offset);
      urlBuilder.append("&Limit=" + pageLimit);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + config.getAccessToken())
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
        current = ((Map<?, ?>) current).get("Response");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("Items");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("offset", offset), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        offset += pageLimit;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollAppresults() throws InterruptedException {
    final String streamName = "appresults";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 1024;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.euw2.sh.basespace.illumina.com/v1pre3/appsessions/{{ stream_partition[\"parent_id\"] }}/appresults");
      urlBuilder.append("?Offset=" + offset);
      urlBuilder.append("&Limit=" + pageLimit);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("Authorization", "Bearer " + config.getAccessToken())
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
        current = ((Map<?, ?>) current).get("Response");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("Items");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("offset", offset), streamName, Schema.STRING_SCHEMA, value));
        }
        if (records.isEmpty()) {
          break;
        }
        offset += pageLimit;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    }
    return allRecords;
  }

  private List<SourceRecord> pollAppresultsFiles() throws InterruptedException {
    final String streamName = "appresults_files";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://api.euw2.sh.basespace.illumina.com/v1pre3/appresults/{{ stream_partition[\"parent_id\"] }}/files");
    try {
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("Authorization", "Bearer " + config.getAccessToken())
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
      current = ((Map<?, ?>) current).get("Response");
      if (current == null) {
        return Collections.emptyList();
      }
      if (!(current instanceof Map)) {
        return Collections.emptyList();
      }
      current = ((Map<?, ?>) current).get("Items");
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
