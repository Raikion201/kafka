package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class PivotalTrackerConnectorConfig extends AbstractConfig {
  public static final String API_TOKEN_CONFIG = "api_token";

  public PivotalTrackerConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(API_TOKEN_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Pivotal Tracker API token");
    return def;
  }

  public String getApiToken() {
    return getString(API_TOKEN_CONFIG);
  }
}
