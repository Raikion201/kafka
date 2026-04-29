package io.kafka.connect.generated;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
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

public final class GoogleAnalyticsJwtSourceTask extends SourceTask {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private GoogleAnalyticsJwtConnectorConfig config;

  private HttpClient httpClient;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new GoogleAnalyticsJwtConnectorConfig(props);
    this.httpClient = HttpClient.newHttpClient();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> all = new ArrayList<>();
    all.addAll(pollReports());
    return all;
  }

  private List<SourceRecord> pollReports() throws InterruptedException {
    final String streamName = "reports";
    List<SourceRecord> result = new ArrayList<>();
    StringBuilder urlBuilder = new StringBuilder("https://analyticsreporting.googleapis.com/v4/reports");
    try {
      String jwtToken = buildJwt();
      HttpRequest request = HttpRequest.newBuilder()
                  .uri(URI.create(urlBuilder.toString()))
                  .header("Authorization", "Bearer " + jwtToken)
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

  private String buildJwt() {
    try {
      Map credsMap = (Map<?, ?>) MAPPER.readValue(config.getCredentialsJson(), Object.class);
      String privateKeyPem = (String) credsMap.get("private_key");
      String keyContent = privateKeyPem
                  .replace("-----BEGIN PRIVATE KEY-----", "")
                  .replace("-----END PRIVATE KEY-----", "")
                  .replace("-----BEGIN RSA PRIVATE KEY-----", "")
                  .replace("-----END RSA PRIVATE KEY-----", "")
                  .replaceAll("\\s+", "");
      byte[] keyBytes = Base64.getDecoder().decode(keyContent);
      long now = System.currentTimeMillis() / 1000L;
      long exp = now + 3600;
      String headerJson = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
      Map<String, Object> payloadMap = new LinkedHashMap<>();
      payloadMap.put("iat", now);
      payloadMap.put("exp", exp);
      payloadMap.put("aud", (String)((Map<?,?>) MAPPER.readValue(config.getCredentialsJson(), Object.class)).get("token_uri"));
      payloadMap.put("iss", (String)((Map<?,?>) MAPPER.readValue(config.getCredentialsJson(), Object.class)).get("client_email"));
      payloadMap.put("scope", "https://www.googleapis.com/auth/analytics.readonly");
      String payloadJson = MAPPER.writeValueAsString(payloadMap);
      String headerB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(headerJson.getBytes(StandardCharsets.UTF_8));
      String payloadB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));
      byte[] signingInput = (headerB64 + "." + payloadB64).getBytes(StandardCharsets.UTF_8);
      PrivateKey privateKey = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(keyBytes));
      Signature sig = Signature.getInstance("SHA256withRSA");
      sig.initSign(privateKey);
      sig.update(signingInput);
      String sigB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(sig.sign());
      return headerB64 + "." + payloadB64 + "." + sigB64;
    } catch (Exception e) {
      throw new ConnectException("Failed to build JWT", e);
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
