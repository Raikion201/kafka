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

public final class XkcdSourceTask extends SourceTask {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private XkcdConnectorConfig config;

  private HttpClient httpClient;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new XkcdConnectorConfig(props);
    this.httpClient = HttpClient.newHttpClient();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> all = new ArrayList<>();
    all.addAll(pollXkcd());
    return all;
  }

  private List<SourceRecord> pollXkcd() throws InterruptedException {
    final String streamName = "xkcd";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://xkcd.com/" + config.getComicNumber() + "/info.0.json");
    try {
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
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
    if (httpClient instanceof AutoCloseable ac) {
      try {
        ac.close();
      } catch (Exception ignored) {
      }
    }
    httpClient = null;
  }
}
