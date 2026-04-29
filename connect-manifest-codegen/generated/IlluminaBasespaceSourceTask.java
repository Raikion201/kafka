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
    return all;
  }

  private List<SourceRecord> pollProjects() throws InterruptedException {
    final String streamName = "projects";
    List<SourceRecord> result = new ArrayList<>();
    Map<String, Object> _stored = context.offsetStorageReader().offset(Map.of("stream", streamName));
    int offset = 0;
    if (_stored != null && _stored.get("offset") instanceof Number _o) {
      offset = _o.intValue();
    }
    final int pageLimit = 100;
    StringBuilder urlBuilder = new StringBuilder("https://api.basespace.illumina.com/v2/projects");
    urlBuilder.append("?offset=" + offset);
    urlBuilder.append("&pageSize=" + pageLimit);
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
        return result;
      }
      current = ((Map<?, ?>) current).get("items");
      if (current == null) {
        return result;
      }
      json = current;
      List<Object> records;
      if (json instanceof List) {
        records = (List<Object>) json;
      } else {
        records = Collections.singletonList(json);
      }
      int nextOffset = records.size() < pageLimit ? 0 : offset + pageLimit;
      for (Object record : records) {
        String value = MAPPER.writeValueAsString(record);
        result.add(new SourceRecord(Map.of("stream", streamName), Map.of("offset", nextOffset), streamName, Schema.STRING_SCHEMA, value));
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
    if (httpClient instanceof AutoCloseable ac) {
      try {
        ac.close();
      } catch (Exception ignored) {
      }
    }
    httpClient = null;
  }
}
