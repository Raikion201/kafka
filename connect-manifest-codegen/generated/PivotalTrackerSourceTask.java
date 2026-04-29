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

public final class PivotalTrackerSourceTask extends SourceTask {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PivotalTrackerConnectorConfig config;

  private HttpClient httpClient;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new PivotalTrackerConnectorConfig(props);
    this.httpClient = HttpClient.newHttpClient();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> all = new ArrayList<>();
    all.addAll(pollProjects());
    all.addAll(pollStories());
    all.addAll(pollProjectMemberships());
    all.addAll(pollLabels());
    all.addAll(pollReleases());
    all.addAll(pollEpics());
    all.addAll(pollActivity());
    return all;
  }

  private List<SourceRecord> pollProjects() throws InterruptedException {
    final String streamName = "projects";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://www.pivotaltracker.com/services/v5/projects");
    try {
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("X-TrackerToken", config.getApiToken())
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

  private List<SourceRecord> pollStories() throws InterruptedException {
    final String streamName = "stories";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 10;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://www.pivotaltracker.com/services/v5/projects/{{ stream_slice['project_id'] }}/stories");
      urlBuilder.append("?offset=" + offset);
      urlBuilder.append("&limit=" + pageLimit);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("X-TrackerToken", config.getApiToken())
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

  private List<SourceRecord> pollProjectMemberships() throws InterruptedException {
    final String streamName = "project_memberships";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://www.pivotaltracker.com/services/v5/projects/{{ stream_slice['project_id'] }}/memberships");
    try {
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("X-TrackerToken", config.getApiToken())
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

  private List<SourceRecord> pollLabels() throws InterruptedException {
    final String streamName = "labels";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://www.pivotaltracker.com/services/v5/projects/{{ stream_slice['project_id'] }}/labels");
    try {
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("X-TrackerToken", config.getApiToken())
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

  private List<SourceRecord> pollReleases() throws InterruptedException {
    final String streamName = "releases";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 10;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://www.pivotaltracker.com/services/v5/projects/{{ stream_slice['project_id'] }}/releases");
      urlBuilder.append("?offset=" + offset);
      urlBuilder.append("&limit=" + pageLimit);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("X-TrackerToken", config.getApiToken())
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

  private List<SourceRecord> pollEpics() throws InterruptedException {
    final String streamName = "epics";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://www.pivotaltracker.com/services/v5/projects/{{ stream_slice['project_id'] }}/epics");
    try {
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("X-TrackerToken", config.getApiToken())
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

  private List<SourceRecord> pollActivity() throws InterruptedException {
    final String streamName = "activity";
    List<SourceRecord> allRecords = new ArrayList<>();
    int offset = 0;
    final int pageLimit = 10;
    while (true) {
      StringBuilder urlBuilder = new StringBuilder("https://www.pivotaltracker.com/services/v5/projects/{{ stream_slice['project_id'] }}/activity");
      urlBuilder.append("?offset=" + offset);
      urlBuilder.append("&limit=" + pageLimit);
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlBuilder.toString()))
                    .header("X-TrackerToken", config.getApiToken())
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
