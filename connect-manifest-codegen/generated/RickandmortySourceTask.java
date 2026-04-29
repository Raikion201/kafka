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

public final class RickandmortySourceTask extends SourceTask {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private RickandmortyConnectorConfig config;

  private HttpClient httpClient;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new RickandmortyConnectorConfig(props);
    this.httpClient = HttpClient.newHttpClient();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> all = new ArrayList<>();
    all.addAll(pollCharacters());
    return all;
  }

  private List<SourceRecord> pollCharacters() throws InterruptedException {
    final String streamName = "characters";
    List<SourceRecord> result = new ArrayList<>();
    Map<String, Object> _stored = context.offsetStorageReader().offset(Map.of("stream", streamName));
    String nextCursor = null;
    if (_stored != null && _stored.get("cursor") instanceof String _c && !_c.isEmpty()) {
      nextCursor = _c;
    }
    String url = null;
    url = (nextCursor != null) ? nextCursor : "https://rickandmortyapi.com/api/character";
    try {
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(url))
                  .GET()
                  .build();
      HttpResponse<String> response = sendWithRetry(request);
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new ConnectException("HTTP " + response.statusCode() + " from " + url);
      }
      Object json = MAPPER.readValue(response.body(), Object.class);
      nextCursor = null;
      Object cursorStep = (Object) json;
      if (cursorStep instanceof Map) {
        cursorStep = ((Map<?, ?>) cursorStep).get("info");
      } else {
        cursorStep = null;
      }
      if (cursorStep instanceof Map) {
        cursorStep = ((Map<?, ?>) cursorStep).get("next");
      } else {
        cursorStep = null;
      }
      nextCursor = cursorStep != null ? String.valueOf(cursorStep) : null;
      Object current = json;
      if (!(current instanceof Map)) {
        return result;
      }
      current = ((Map<?, ?>) current).get("results");
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
      for (Object record : records) {
        String value = MAPPER.writeValueAsString(record);
        result.add(new SourceRecord(Map.of("stream", streamName), Map.of("cursor", nextCursor != null ? nextCursor : ""), streamName, Schema.STRING_SCHEMA, value));
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
