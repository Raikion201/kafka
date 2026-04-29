package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class NewsapiConnectorConfig extends AbstractConfig {
  public static final String API_KEY_CONFIG = "api_key";

  public NewsapiConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(API_KEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "NewsAPI.org API key. Get one free (no credit card) at https://newsapi.org/register");
    return def;
  }

  public String getApiKey() {
    return getString(API_KEY_CONFIG);
  }
}
