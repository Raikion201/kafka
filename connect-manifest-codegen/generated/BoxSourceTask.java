package io.kafka.connect.generated;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public final class BoxSourceTask extends SourceTask {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private BoxConnectorConfig config;

  private HttpClient httpClient;

  private volatile String cachedToken;

  private volatile long tokenExpiryMs = 0L;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new BoxConnectorConfig(props);
    this.httpClient = HttpClient.newHttpClient();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> all = new ArrayList<>();
    all.addAll(pollEvents());
    all.addAll(pollSignTemplates());
    all.addAll(pollCollections());
    all.addAll(pollCollectionItems());
    all.addAll(pollSignRequest());
    all.addAll(pollAdminLogs());
    all.addAll(pollFiles());
    all.addAll(pollFileCollaborations());
    all.addAll(pollFolderCollaborations());
    all.addAll(pollFileComments());
    all.addAll(pollRecentItems());
    all.addAll(pollFileTasks());
    all.addAll(pollTrashedItems());
    all.addAll(pollUsers());
    all.addAll(pollFolders());
    return all;
  }

  private List<SourceRecord> pollEvents() throws InterruptedException {
    final String streamName = "events";
    List<SourceRecord> allRecords = new ArrayList<>();
    String nextCursor = null;
    do {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/events");
      if (nextCursor != null) {
        urlBuilder.append("?stream_position=" + nextCursor);
      }
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("cursor", nextCursor != null ? nextCursor : ""), streamName, Schema.STRING_SCHEMA, value));
        }
        nextCursor = null;
        Object cursorStep = (Object) json;
        if (cursorStep instanceof Map) {
          cursorStep = ((Map<?, ?>) cursorStep).get("next_stream_position");
        } else {
          cursorStep = null;
        }
        nextCursor = cursorStep != null ? String.valueOf(cursorStep) : null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    } while (nextCursor != null);
    return allRecords;
  }

  private List<SourceRecord> pollSignTemplates() throws InterruptedException {
    final String streamName = "sign_templates";
    List<SourceRecord> allRecords = new ArrayList<>();
    String nextCursor = null;
    do {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/sign_templates");
      if (nextCursor != null) {
        urlBuilder.append("?marker=" + nextCursor);
      }
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("cursor", nextCursor != null ? nextCursor : ""), streamName, Schema.STRING_SCHEMA, value));
        }
        nextCursor = null;
        if (json instanceof Map) {
          Map<?, ?> respMap = (Map<?, ?>) json;
          Object nextToken = respMap.get("next_page_token");
          nextCursor = nextToken != null ? String.valueOf(nextToken) : null;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    } while (nextCursor != null);
    return allRecords;
  }

  private List<SourceRecord> pollCollections() throws InterruptedException {
    final String streamName = "collections";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 100;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/collections");
      urlBuilder.append("?offset=" + offset);
      urlBuilder.append("&limit=" + pageLimit);
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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

  private List<SourceRecord> pollCollectionItems() throws InterruptedException {
    final String streamName = "collection_items";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 0;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/collections/{{ stream_partition.collection }}/items");
      urlBuilder.append("?offset=" + offset);
      urlBuilder.append("&per_page=" + pageLimit);
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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

  private List<SourceRecord> pollSignRequest() throws InterruptedException {
    final String streamName = "sign_request";
    List<SourceRecord> allRecords = new ArrayList<>();
    String nextCursor = null;
    do {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/sign_requests");
      if (nextCursor != null) {
        urlBuilder.append("?marker=" + nextCursor);
      }
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("cursor", nextCursor != null ? nextCursor : ""), streamName, Schema.STRING_SCHEMA, value));
        }
        nextCursor = null;
        Object cursorStep = (Object) json;
        if (cursorStep instanceof Map) {
          cursorStep = ((Map<?, ?>) cursorStep).get("next_marker");
        } else {
          cursorStep = null;
        }
        nextCursor = cursorStep != null ? String.valueOf(cursorStep) : null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    } while (nextCursor != null);
    return allRecords;
  }

  private List<SourceRecord> pollAdminLogs() throws InterruptedException {
    final String streamName = "admin_logs";
    List<SourceRecord> allRecords = new ArrayList<>();
    String nextCursor = null;
    do {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/events?stream_type=admin_logs");
      if (nextCursor != null) {
        urlBuilder.append("?stream_position=" + nextCursor);
      }
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("cursor", nextCursor != null ? nextCursor : ""), streamName, Schema.STRING_SCHEMA, value));
        }
        nextCursor = null;
        Object cursorStep = (Object) json;
        if (cursorStep instanceof Map) {
          cursorStep = ((Map<?, ?>) cursorStep).get("next_stream_position");
        } else {
          cursorStep = null;
        }
        nextCursor = cursorStep != null ? String.valueOf(cursorStep) : null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    } while (nextCursor != null);
    return allRecords;
  }

  private List<SourceRecord> pollFiles() throws InterruptedException {
    final String streamName = "files";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 100;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/folders/0/items");
      urlBuilder.append("?offset=" + offset);
      urlBuilder.append("&limit=" + pageLimit);
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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

  private List<SourceRecord> pollFileCollaborations() throws InterruptedException {
    final String streamName = "file_collaborations";
    List<SourceRecord> allRecords = new ArrayList<>();
    String nextCursor = null;
    do {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/files/{{ stream_slice['file_id'] }}/collaborations");
      if (nextCursor != null) {
        urlBuilder.append("?marker=" + nextCursor);
      }
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("cursor", nextCursor != null ? nextCursor : ""), streamName, Schema.STRING_SCHEMA, value));
        }
        nextCursor = null;
        Object cursorStep = (Object) json;
        if (cursorStep instanceof Map) {
          cursorStep = ((Map<?, ?>) cursorStep).get("next_marker");
        } else {
          cursorStep = null;
        }
        nextCursor = cursorStep != null ? String.valueOf(cursorStep) : null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    } while (nextCursor != null);
    return allRecords;
  }

  private List<SourceRecord> pollFolderCollaborations() throws InterruptedException {
    final String streamName = "folder_collaborations";
    List<SourceRecord> allRecords = new ArrayList<>();
    String nextCursor = null;
    do {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/folders/{{ stream_slice['folder_id'] }}/collaborations");
      if (nextCursor != null) {
        urlBuilder.append("?marker=" + nextCursor);
      }
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("cursor", nextCursor != null ? nextCursor : ""), streamName, Schema.STRING_SCHEMA, value));
        }
        nextCursor = null;
        Object cursorStep = (Object) json;
        if (cursorStep instanceof Map) {
          cursorStep = ((Map<?, ?>) cursorStep).get("next_marker");
        } else {
          cursorStep = null;
        }
        nextCursor = cursorStep != null ? String.valueOf(cursorStep) : null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    } while (nextCursor != null);
    return allRecords;
  }

  private List<SourceRecord> pollFileComments() throws InterruptedException {
    final String streamName = "file_comments";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 100;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/files/{{ stream_slice['file_id'] }}/comments");
      urlBuilder.append("?offset=" + offset);
      urlBuilder.append("&limit=" + pageLimit);
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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

  private List<SourceRecord> pollRecentItems() throws InterruptedException {
    final String streamName = "recent_items";
    List<SourceRecord> allRecords = new ArrayList<>();
    String nextCursor = null;
    do {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/recent_items");
      if (nextCursor != null) {
        urlBuilder.append("?marker=" + nextCursor);
      }
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("*");
        if (current == null) {
          return allRecords;
        }
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("item");
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("cursor", nextCursor != null ? nextCursor : ""), streamName, Schema.STRING_SCHEMA, value));
        }
        nextCursor = null;
        Object cursorStep = (Object) json;
        if (cursorStep instanceof Map) {
          cursorStep = ((Map<?, ?>) cursorStep).get("next_marker");
        } else {
          cursorStep = null;
        }
        nextCursor = cursorStep != null ? String.valueOf(cursorStep) : null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while polling " + streamName, e);
      } catch (Exception e) {
        throw new ConnectException("Failed to poll " + streamName, e);
      }
    } while (nextCursor != null);
    return allRecords;
  }

  private List<SourceRecord> pollFileTasks() throws InterruptedException {
    final String streamName = "file_tasks";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/files/{{ stream_slice['file_id'] }}/tasks");
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
      current = ((Map<?, ?>) current).get("entries");
      if (current == null) {
        return Collections.emptyList();
      }
      if (!(current instanceof Map)) {
        return Collections.emptyList();
      }
      current = ((Map<?, ?>) current).get("*");
      if (current == null) {
        return Collections.emptyList();
      }
      if (!(current instanceof Map)) {
        return Collections.emptyList();
      }
      current = ((Map<?, ?>) current).get("task_assignment_collection");
      if (current == null) {
        return Collections.emptyList();
      }
      if (!(current instanceof Map)) {
        return Collections.emptyList();
      }
      current = ((Map<?, ?>) current).get("entries");
      if (current == null) {
        return Collections.emptyList();
      }
      if (!(current instanceof Map)) {
        return Collections.emptyList();
      }
      current = ((Map<?, ?>) current).get("*");
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

  private List<SourceRecord> pollTrashedItems() throws InterruptedException {
    final String streamName = "trashed_items";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 1000;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/folders/trash/items");
      urlBuilder.append("?offset=" + offset);
      urlBuilder.append("&per_page=" + pageLimit);
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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

  private List<SourceRecord> pollUsers() throws InterruptedException {
    final String streamName = "users";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 0;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/users");
      urlBuilder.append("?offset=" + offset);
      urlBuilder.append("&per_page=" + pageLimit);
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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

  private List<SourceRecord> pollFolders() throws InterruptedException {
    final String streamName = "folders";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 100;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://api.box.com/2.0/folders/0/items");
      urlBuilder.append("?offset=" + offset);
      urlBuilder.append("&limit=" + pageLimit);
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
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("entries");
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

  private String refreshAccessToken() {
    if (cachedToken != null && System.currentTimeMillis() < tokenExpiryMs) {
      return cachedToken;
    }
    String reqBody = "grant_type=client_credentials"
                + "&client_id=" + config.getClientId()
                + "&client_secret=" + config.getClientSecret()
                + "&box_subject_id=" + config.getUser()
                + "&box_subject_type=user";
    try {
      HttpRequest tokenRequest = HttpRequest.newBuilder()
                  .uri(URI.create("https://api.box.com/oauth2/token"))
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
    httpClient = null;
  }
}
