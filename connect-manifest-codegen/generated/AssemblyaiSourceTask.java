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

public final class AssemblyaiSourceTask extends SourceTask {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private AssemblyaiConnectorConfig config;

  private HttpClient httpClient;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new AssemblyaiConnectorConfig(props);
    this.httpClient = HttpClient.newHttpClient();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> all = new ArrayList<>();
    all.addAll(pollTranscripts());
    all.addAll(pollTranscriptSentences());
    all.addAll(pollParagraphs());
    all.addAll(pollTranscriptSubtitle());
    all.addAll(pollLemurResponse());
    return all;
  }

  private List<SourceRecord> pollTranscripts() throws InterruptedException {
    final String streamName = "transcripts";
    List<SourceRecord> allRecords = new ArrayList<>();
    String nextCursor = null;
    String url = null;
    do {
      url = (nextCursor != null) ? nextCursor : "https://api.assemblyai.com/v2/transcript";
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "Bearer " + config.getApiKey())
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + url);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("transcripts");
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
          cursorStep = ((Map<?, ?>) cursorStep).get("page_details");
        } else {
          cursorStep = null;
        }
        if (cursorStep instanceof Map) {
          cursorStep = ((Map<?, ?>) cursorStep).get("next_url");
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

  private List<SourceRecord> pollTranscriptSentences() throws InterruptedException {
    final String streamName = "transcript_sentences";
    List<SourceRecord> allRecords = new ArrayList<>();
    String nextCursor = null;
    String url = null;
    do {
      url = (nextCursor != null) ? nextCursor : "https://api.assemblyai.com/v2/transcript/{{ stream_partition['tr_id'] }}/sentences";
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "Bearer " + config.getApiKey())
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + url);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("sentences");
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
          cursorStep = ((Map<?, ?>) cursorStep).get("page_details");
        } else {
          cursorStep = null;
        }
        if (cursorStep instanceof Map) {
          cursorStep = ((Map<?, ?>) cursorStep).get("next_url");
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

  private List<SourceRecord> pollParagraphs() throws InterruptedException {
    final String streamName = "paragraphs";
    List<SourceRecord> allRecords = new ArrayList<>();
    String nextCursor = null;
    String url = null;
    do {
      url = (nextCursor != null) ? nextCursor : "https://api.assemblyai.com/v2/transcript/{{ stream_partition['tr_id'] }}/paragraphs";
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "Bearer " + config.getApiKey())
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + url);
        }
        Object json = MAPPER.readValue(response.body(), Object.class);
        Object current = json;
        if (!(current instanceof Map)) {
          return allRecords;
        }
        current = ((Map<?, ?>) current).get("paragraphs");
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
          cursorStep = ((Map<?, ?>) cursorStep).get("page_details");
        } else {
          cursorStep = null;
        }
        if (cursorStep instanceof Map) {
          cursorStep = ((Map<?, ?>) cursorStep).get("next_url");
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

  private List<SourceRecord> pollTranscriptSubtitle() throws InterruptedException {
    final String streamName = "transcript_subtitle";
    List<SourceRecord> allRecords = new ArrayList<>();
    String nextCursor = null;
    String url = null;
    do {
      url = (nextCursor != null) ? nextCursor : "https://api.assemblyai.com/v2/transcript/{{ stream_partition['tr_id'] }}/redacted-audio";
      try {
        HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "Bearer " + config.getApiKey())
                    .GET()
                    .build();
        HttpResponse<String> response = sendWithRetry(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
          throw new ConnectException("HTTP " + response.statusCode() + " from " + url);
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
          allRecords.add(new SourceRecord(Map.of("stream", streamName), Map.of("cursor", nextCursor != null ? nextCursor : ""), streamName, Schema.STRING_SCHEMA, value));
        }
        nextCursor = null;
        Object cursorStep = (Object) json;
        if (cursorStep instanceof Map) {
          cursorStep = ((Map<?, ?>) cursorStep).get("page_details");
        } else {
          cursorStep = null;
        }
        if (cursorStep instanceof Map) {
          cursorStep = ((Map<?, ?>) cursorStep).get("next_url");
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

  private List<SourceRecord> pollLemurResponse() throws InterruptedException {
    final String streamName = "lemur_response";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://api.assemblyai.com" + "/lemur/v3/" + config.getRequestId());
    try {
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("Authorization", "Bearer " + config.getApiKey())
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
